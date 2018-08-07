#!/usr/bin/env python
import sys
import os
import time
import signal
import pyes
import json
import logging
from swiftclient.client import HTTPConnection
from swiftclient import client
from urlparse import urlparse
from copy import copy

from swift.common.utils import get_logger, drop_privileges
from swift.common.daemon import Daemon
from swiftclient import get_account, get_container, head_object, \
    ClientException, get_auth_1_0, encode_utf8

from swearch.index import ObjectUpdateHandler, ContainerUpdateHandler, \
    get_search_client
from swearch.rabbit import RabbitMQElasticIndexer



class IndexWorker(Daemon):
    """
    Index worker daemon that pops messages from the backlog and indexes them.

    :param conf: The daemon configuration.
    """

    def __init__(self, conf, pid=None, key=None, user='swift', queue='object'):
        self.queue_config = conf.get('queue', {})
        self.index_config = conf.get('index', {})
        self.conf = self.index_config

        self.index = self.index_config.get('search_index_name', 'os_default')

        self.key = key
        self.user = user
        self.pidfile = pid
        self.logger = get_logger(
            self.index_config,
            name='swearch-%s' % queue,
            log_route='swearch-%s' % queue,
            fmt="%(server)s [PID: %(process)d] %(message)s")
        self.indexer = None
        self.auth_token = None

        self.container_listing_count = int(
            self.queue_config.get('container_listing_count', 5000))

        self.queue_type = queue

        self.highwater_name = {
            'object': 'river',
            'container': 'object',
            'account': 'container',
        }.get(self.queue_type)

        self.highwater = int(self.queue_config.get(
            "%s_highwater" % self.highwater_name, 500000))
        self.highwater_ok = int(self.queue_config.get(
            "%s_highwater_ok" % self.highwater_name, 10000))

        self.message_handler = {
            'index_account': self.index_account,
            'index_container': self.index_container,
            'index_object': self.index_object,
        }

        self.prefetch = int(self.queue_config.get(
            '%s_prefetch_count' % self.queue_type, 100))

        self.waterlevel_interval = int(self.queue_config.get(
            "%s_waterlevel_interval" % self.highwater_name, 3))

        self.indexer = RabbitMQElasticIndexer(self.queue_config)
        self.auth_token = self.get_auth_token(self.key)

        self.highwater_queue = {
            'object': self.indexer.rabbitmq_queue,
            'container': self.indexer.backfill_queue_name(self.highwater_name),
            'account': self.indexer.backfill_queue_name(self.highwater_name),
        }.get(self.queue_type)

    def run_forever(self, *args, **kwargs):
        while True:
            try:
                # before any action is taken, check the river backlog so
                # we don't overflow the river and flood elastic search.
                for messages in self.queue_highwater():
                    time.sleep(self.waterlevel_interval)

                self.indexer.start_consumer(
                    self.queue_type, self.handle_messages,
                        prefetch=int(self.prefetch))
            finally:
                self.indexer.close_mq_client()

    def handle_messages(self, body):
        """A try/except block was added to capture errors within JSON structure during backfills """
        try:
            task = json.loads(body)
            f = self.message_handler.get(task['type'])
	    if f:
                try:
                    f(*task['args'], **task['kwargs'])
                except ClientException as e:
                    if e.http_status == 401:
                        self.auth_token = self.get_auth_token(self.key)
                        f(*task['args'], **task['kwargs'])
                    elif e.http_status == 404:
                        pass
            else:
                self.logger.error("INVALID ACTION: %s" % (task))
        except:
            print 'Unexpected error: %s' % sys.exc_info()[0]
            print 'The body: %s' % body


        #if f:
        #    try:
        #        f(*task['args'], **task['kwargs'])
        #    except ClientException as e:
        #        if e.http_status == 401:
        #            self.auth_token = self.get_auth_token(self.key)
        #            f(*task['args'], **task['kwargs'])
        #        elif e.http_status == 404:
        #            pass
        #else:
        #    self.logger.error("INVALID ACTION: %s" % (task))

    def queue_highwater(self):
        was_highwater = False
        while True:
            water_level = self.indexer.queue_size(self.highwater_queue)

            if not was_highwater and water_level > self.highwater:
                if not was_highwater:
                    self.logger.warning(
                        "'%s' hit high water mark of "
                        "%d/%d messages.  Suspending processing." %
                        (self.highwater_name, water_level, self.highwater))
                was_highwater = True
                yield copy(water_level)
            elif was_highwater and water_level > self.highwater_ok:
                self.logger.warning(
                    "Water '%s' level still too high %d/%d" %
                    (self.highwater_name, water_level, self.highwater_ok))
                yield copy(water_level)
            else:
                if was_highwater:
                    self.logger.warning(
                        "'%s' high water recovered. %s messages pending" %
                        (self.highwater_name, water_level))
                break

    def start(self):
        # Check for a pidfile to see if the daemon already runs
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if pid:
            message = "pidfile %s already exist. Daemon already running?\n"
            sys.stderr.write(message % self.pidfile)
            sys.exit(1)

        # Start the daemon
        pid = daemonize(self.pidfile, user=self.user)
        self.logger.notice('Started child %s' % pid)
        self.run()

    def stop(self):
        # Get the pid from the pidfile
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if not pid:
            message = "pidfile %s does not exist. Daemon not running?\n"
            sys.stderr.write(message % self.pidfile)
            return  # not an error in a restart

        # Try killing the daemon process
        try:
            # Try to terminate gracefully with SIGTERM
            i = 0
            while i < 30:
                os.kill(pid, signal.SIGTERM)
                time.sleep(0.1)
                i += 1

            # Okay, kill it with fire
            i = 0
            while i < 5:
                os.kill(pid, signal.SIGKILL)
                time.sleep(0.1)
                i += 1
        except OSError, err:
            err = str(err)
            if err.find("No such process") > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                sys.exit(1)

    def restart(self):
        self.stop()
        self.start()

    def _get_conn(self, url, timeout=10):
        url = encode_utf8(url)
        parsed = urlparse(url)
        """Uses the swiftclient HTTPConnection to create a connection which handles both HTTP and HTTPS
         and passes in a swearch True boolean """
        conn = client.HTTPConnection(url, swearch=True)
        conn.timeout = timeout
        return parsed, conn

    def index_account(self, account, verify=False):
        "List all containers for the account and create task for each"
        self.logger.debug("Indexing Account %s" % (account))
        url = '/'.join(['http://127.0.0.1', 'v1', account])
        headers, containers = get_account(url, self.auth_token,
                                          full_listing=True,
                                          http_conn=self._get_conn(url))

        self.logger.info("Found %s Container(s) in %s" %
                         (len(containers), account.encode('utf8')))

        for container in containers:
            self.indexer.publish_backfill_task(
                'container', account, container['name'], verify=verify)

    def index_container(self, account, container, marker=None, verify=False):
        "List all objects for the container and create task for each"
        self.logger.debug("Indexing container %s/%s; marker=%s" % (
            account.encode('utf8'),
            container.encode('utf8'),
            (marker or '').encode('utf8')))
        # Index the container up to marker
        url = '/'.join(['http://127.0.0.1', 'v1', account])
        headers, objects = get_container(url, self.auth_token, container,
                                         limit=self.container_listing_count,
                                         marker=marker,
                                         http_conn=self._get_conn(url))

        self.logger.info("Found %s Objects(s) in container %s" %
                         (len(objects), container.encode('utf8')))

        if len(objects) == self.container_listing_count:
            self.indexer.publish_backfill_task(
                'container', account, container,
                marker=objects[-1]['name'],
                verify=verify)

        update_handler = ContainerUpdateHandler(
            self.indexer, account, container, index=self.index)
        _id, props = update_handler.parse_props(headers)

        if verify:
            # Get Indexed State of Container
            client = get_search_client(self.index_config)
            results = client.search(
                query=pyes.TermQuery("_id", _id),
                indices=[self.index_config.get(
                    'search_index_name', 'os_default')])
            assert results.total == 1, '%s indexed instead of 1' % \
                results.total
            for name, val in results[0].iteritems():
                if encode_utf8(val) != encode_utf8(props[name]):
                    self.logger.error('Indexed Property Does Not Match')
                    self.logger.error(val)
                    self.logger.error(props[name])
        else:
            update_handler.index(_id, props, block=True)

        # List all of the objects in the container
        self.logger.info(
            "Found %s Objects in %s/%s" %
            (len(objects), account.encode('utf8'), container.encode('utf8')))
        for obj in objects:
            self.indexer.publish_backfill_task(
                'object', account, container, obj['name'], verify=verify)

    def index_object(self, account, container, obj, verify=False):
        "Get details for object and index it"
        url = '/'.join(['http://127.0.0.1', 'v1', account])
        headers = head_object(url, self.auth_token, container, obj,
                              http_conn=self._get_conn(url))

        self.logger.debug("Indexing Object: %s/%s/%s" % (
            account.encode('utf8'),
            container.encode('utf8'),
            obj.encode('utf8')))

        update_handler = ObjectUpdateHandler(
            self.indexer, account, container, obj, index=self.index)
        _id, props = update_handler.parse_props(headers)

        if verify:
            # Check Indexed State of Object
            client = get_search_client(self.index_config)
            _id, props = update_handler.parse_props(headers)
            q = pyes.TermQuery("_id", _id)
            results = client.search(
                query=q, indices=[self.index_config.get(
                    'search_index_name', 'os_default')])
            assert results.total == 1, '%s indexed instead of 1' % \
                results.total
            for name, val in results[0].iteritems():
                if encode_utf8(val) != encode_utf8(props[name]):
                    self.logger.error('Indexed Property Does Not Match')
                    self.logger.error(val)
                    self.logger.error(props[name])
        else:
            update_handler.index(_id, props, block=True)

    def get_auth_token(self, key):
        "get auth token to use for index requests"
        storage_url, token = get_auth_1_0(
            'http://127.0.0.1/auth/v1.0', '.super_admin:.super_admin', key,
            False)
        return token


def daemonize(pidfile, user=None):
    try:
        pid = os.fork()
        if pid > 0:
            # exit first parent
            sys.exit(0)
    except OSError, e:
        sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
        sys.exit(1)

    # decouple from parent environment
    if user:
        drop_privileges(user)

    # do second fork
    try:
        pid = os.fork()
        if pid > 0:
            # exit from second parent
            sys.exit(0)
    except OSError, e:
        sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
        sys.exit(1)

    pid = str(os.getpid())
    file(pidfile, 'w+').write("%s\n" % pid)
    return pid
