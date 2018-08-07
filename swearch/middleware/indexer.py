"""
    Indexer Middleware module
"""
import sys
import traceback

from swift.common import swob
from swift.common import utils as swift_utils

from swearch import index
from swearch import rabbit
from swearch import utils


class IndexMiddleware(object):
    """Indexer Middleware.

    This will use POST, DELETE, PATCH and PUT to the proxy commands to decide
    which items need indexing. It will then defer the index task to an index
    queue.
    """

    def __init__(self, app, conf, *args, **kwargs):
        self.logger = swift_utils.get_logger(conf, log_route='swearch')

        config_file = conf.get('config_file', '/etc/swift/swearch.conf')
        config = swift_utils.readconf(config_file)

        self.queue_config = config.get('queue', {})
        self.index_config = config.get('index', {})
        self.app = app
        self.indexer = rabbit.RabbitMQElasticIndexer(self.queue_config)

    def __call__(self, env, start_response):
        req = swob.Request(env)

        def my_start_response(status, headers, exc_info=None):
            # Register a posthook to index requests
            status_int = int(status.split(' ', 1)[0])
            if 'eventlet.posthooks' in env:
                env['eventlet.posthooks'].append(
                    (self.index_request, (req, status_int), {}))
            else:
                # If I don't have the ability to register a posthook,
                # the response hasn't been fullfilled yet
                try:
                    self.index_request(env, req, status_int)
                except Exception:
                    traceback.print_exc(file=sys.stdout)
            start_response(status, headers, exc_info)

        return self.app(env, my_start_response)

    def index_request(self, env, req, status):
        """Index the request by creating a task in the indexer."""

        if status // 100 is not 2:
            return
        if req.method not in ['DELETE', 'PUT', 'POST', 'COPY']:
            return

        version, account, container, obj = swift_utils.split_path(req.path,
                                                                  1, 4, True)

        if container:
            container = utils.unicode_unquote(container)
        if obj:
            obj = utils.unicode_unquote(obj)

        if account == 'AUTH_.auth':
            return

        if obj and container and account:
            update_handler = index.ObjectUpdateHandler(
                self.indexer,
                account,
                container,
                obj,
                index=self.index_config.get('search_index_name'),
                app=self.app,
                logger=self.logger)
        elif container and account:
            update_handler = index.ContainerUpdateHandler(
                self.indexer,
                account,
                container,
                index=self.index_config.get('search_index_name'),
                app=self.app,
                logger=self.logger)
        else:
            update_handler = None

        if update_handler:
            handler = {
                "DELETE": update_handler.after_DELETE,
                "PUT": update_handler.after_PUT,
                "POST": update_handler.after_POST,
                "COPY": update_handler.after_COPY,
            }.get(req.method)

            if handler:
                self.logger.info("Indexing request: %s %s" % (req.method,
                                                              req.path))
                handler(req)


def filter_factory(global_conf, **local_conf):
    """Returns a WSGI filter app for use with paste.deploy."""
    conf = global_conf.copy()
    conf.update(local_conf)

    def index_filter(app):
        """Index middleware filter function."""
        return IndexMiddleware(app, conf)
    return index_filter
