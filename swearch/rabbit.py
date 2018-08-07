import json
import random
import sys
import traceback

import eventlet
import pika
from swift.common import utils


class RabbitMQElasticIndexer(object):
    def __init__(self, conf):
        # RabbitMQ Config
        self.logger = utils.get_logger(conf, log_route='swearch-rabbit')
        urls_str = conf.get('rabbitmq_hosts', '127.0.0.1:5672')
        urls = [x.strip() for x in urls_str.split(',')]
        self.rabbitmq_hosts = []
        for url in urls:
            host, port = url.split(':')
            self.rabbitmq_hosts.append((host, int(port)))

        self.rabbitmq_user = conf.get('rabbitmq_user', 'elasticsearch')
        self.rabbitmq_password = conf.get('rabbitmq_password', 'elasticsearch')
        self.rabbitmq_vhost = conf.get('rabbitmq_vhost', 'swift')
        self.rabbitmq_buffer_size = int(conf.get('rabbitmq_buffer_size',
                                                 '50000').strip())

        # Set up connection params
        credentials = pika.PlainCredentials(
            self.rabbitmq_user, self.rabbitmq_password)

        self.rabbitmq_conn_params = []
        for host, port in self.rabbitmq_hosts:
            self.rabbitmq_conn_params.append(pika.ConnectionParameters(
                host=host, port=int(port), credentials=credentials,
                virtual_host=self.rabbitmq_vhost,
                connection_attempts=2, heartbeat_interval=20,
            ))

        # Lazy loaded
        self.connection = None
        self.channel = None

        self.require_confirmations = (conf.get(
            'rabbitmq_require_confirmations',
            'no').lower() in utils.TRUE_VALUES)
        self.send_attempts = int(
            conf.get('rabbitmq_send_attempts', '2').strip())

        self.rabbitmq_exchange = conf.get(
            'rabbitmq_exchange', 'search.exchange')
        self.rabbitmq_queue = conf.get('rabbitmq_queue', 'elasticsearch')
        self.rabbitmq_routing_key = conf.get('rabbitmq_routing_key', 'search')

        self.rabbitmq_backfill_exchange = conf.get(
            'rabbitmq_backfill_exchange', 'search.backfill')
        self.rabbitmq_backfill_queue = conf.get(
            'rabbitmq_backfill_queue', 'search.backfill')

        # Heartbeat greenlet
        self.publish_queue = eventlet.Queue(self.rabbitmq_buffer_size)
        self.heartbeat = eventlet.spawn(self._heartbeat)
        self.publisher = eventlet.spawn(self._publisher)

        self._timeout = None

    def backfill_queue_name(self, queue):
        return '.'.join([self.rabbitmq_backfill_queue, queue])

    def _heartbeat(self):
        while True:
            try:
                if self.connection and self.connection.is_open:
                    self.connection.process_data_events()
                elif self.connection and not self.connection.is_open:
                    self.connection = self.channel = None
            except Exception:
                self.connection = self.channel = None
                traceback.print_exc(file=sys.stderr)
                self.logger.error('Failed heartbeat.')
                eventlet.sleep(60)
            else:
                eventlet.sleep(4)

    def _publisher(self):
        """Wrapper for _publish_retry, meant to be spawned as a coroutine."""
        while True:
            self._publish_retry()

    def _publish_retry(self):
        """Blocking call with reconnect logic for publishing a single message.

        Reduces the likelihood of dropping an indexer message.
        """

        kwargs = self.publish_queue.get(True)
        conn, chan = self.get_mq_client()
        for i in range(self.send_attempts):
            try:
                result = chan.basic_publish(**kwargs)
                if result:
                    # message sent successfully
                    self.publish_queue.task_done()
                    break

                self.logger.error(
                    'Unable to confirm delivery of message '
                    'to rabbit: %s'
                    % kwargs)
                self.get_mq_client(reconnect=True)
            except Exception:
                self.logger.exception(
                    'Unable to deliver message to rabbit: %s' % kwargs)
                self.get_mq_client(reconnect=True)
        else:
            self.publish_queue.task_done()
            self.logger.error(
                'Giving up delivering message to rabbit: %s' % kwargs)

    def _publish(self, block=False, **kwargs):
        try:
            self.publish_queue.put(kwargs, block)
        except Exception:
            self.logger.exception("Unable to queue message for publishing to "
                                  "rabbitmq. Messages have been lost!")
        else:
            return True

        return False

    def index_doc(self, index_name, _id, props, block=False):
        meta_props = {
            "_index": index_name,
            "_type": "entity",
            "_id": _id
        }
        doc = "%s\n%s\n" % (
            json.dumps({"index": meta_props}),
            json.dumps(props)
        )
        self._publish(
            block=block,
            exchange=self.rabbitmq_exchange,
            mandatory=True,
            routing_key=self.rabbitmq_routing_key,
            body=doc,
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            ),
        )

    def remove_doc(self, index_name, _id):
        meta_props = {
            "_index": index_name,
            "_type": "entity",
            "_id": _id
        }
        doc = "%s\n" % (json.dumps({"delete": meta_props}))

        self._publish(
            exchange=self.rabbitmq_exchange,
            mandatory=True,
            routing_key=self.rabbitmq_routing_key,
            body=doc,
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            ),
        )

    def publish_backfill_task(self, _type, *args, **kwargs):
        conn, channel = self.get_mq_client()
        channel.basic_publish(
            exchange=self.rabbitmq_backfill_exchange,
            mandatory=True,
            routing_key=_type,
            body=json.dumps(
                {'type': 'index_' + _type, 'args': args, 'kwargs': kwargs}),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            ),
        )

    def backfill_queue_size(self, queue):
        queue = self.backfill_queue_name(queue)
        return self.queue_size(queue)

    def queue_size(self, name):
        conn, channel = self.get_mq_client()
        try:
            q = channel.queue_declare(queue=name, passive=True)
        except KeyError:
            return -1
        if not q:
            return -1
        return q.method.message_count

    def setup_river(self):
        conn, channel = self.get_mq_client()
        queue = self.rabbitmq_queue
        exchange = self.rabbitmq_exchange
        channel.queue_declare(queue=queue,
                              durable=True,
                              arguments={"x-ha-policy": "all"})
        channel.exchange_declare(exchange=exchange, durable=True)
        channel.queue_bind(queue=queue,
                           exchange=exchange,
                           routing_key=self.rabbitmq_routing_key)

    def setup_backfill(self):
        conn, channel = self.get_mq_client()
        exchange = self.rabbitmq_backfill_exchange
        channel.exchange_declare(exchange=exchange, durable=True)
        for queue in ['object', 'container', 'account']:
            queue_name = self.backfill_queue_name(queue)
            channel.queue_declare(queue=queue_name,
                                  durable=True,
                                  arguments={"x-ha-policy": "all"})
            channel.queue_bind(queue=queue_name,
                               exchange=exchange,
                               routing_key=queue)

    def consume_one(self, queue, handler):
        conn, channel = self.get_mq_client()
        queue_name = self.backfill_queue_name(queue)
        method_frame, header_frame, body = channel.basic_get(queue=queue_name)
        if method_frame:
            try:
                handler(body)
            except Exception:
                channel.basic_nack(method_frame.delivery_tag)
                traceback.print_exc(file=sys.stderr)
            else:
                channel.basic_ack(method_frame.delivery_tag)

    def start_consumer(self, queue, handler, prefetch=100):
        conn, _ = self.get_mq_client()
        queue_name = self.backfill_queue_name(queue)
        fetched = 0

        channel = conn.channel()
        channel.basic_qos(prefetch_count=prefetch)

        try:
            messages = channel.consume(queue_name)
            for method_frame, properties, body in messages:
                if fetched >= prefetch:
                    channel.cancel()
                    break

                try:
                    handler(body)
                except Exception:
                    traceback.print_exc(file=sys.stderr)
                    channel.basic_nack(method_frame.delivery_tag, requeue=True)
                    raise
                else:
                    channel.basic_ack(method_frame.delivery_tag)
                finally:
                    fetched += 1
        except Exception:
            traceback.print_exc(file=sys.stderr)
        finally:
            if channel.is_open:
                channel.close()
            channel._generator_messages = []

    def get_mq_client(self, reconnect=False):
        if reconnect:
            self.connection = None
            self.channel = None

        if self.connection and self.channel:
            return self.connection, self.channel

        # Pick a random rabbit server
        random.shuffle(self.rabbitmq_conn_params)
        for params in self.rabbitmq_conn_params:
            self.logger.info(
                'Connecting to RabbitMQ using params: %s' % params)
            try:
                self.connection = pika.BlockingConnection(params)
                self.channel = self.connection.channel()
                if self.require_confirmations:
                    self.channel.confirm_delivery()
                return self.connection, self.channel
            except pika.exceptions.AMQPError as amqp:
                self.logger.error(
                    'Can not connect to rabbit(%r). Reason: %r'
                    % (params, amqp))
            except Exception as err:
                traceback.print_exc(file=sys.stderr)
                self.logger.critical(
                    'Failed to connect to rabbit. Reason: %s' % err)

    def close_mq_client(self):
        if self.connection and self.channel:
            self.connection.close()
            self.channel = self.connection = None
        self.heartbeat.cancel()
