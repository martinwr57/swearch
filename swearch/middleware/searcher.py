"""
    Searcher Middleware
"""
import json
import socket
import sys
import traceback
from xml.sax import saxutils

from swift.common import swob
from swift.common import utils as swift_utils

from swearch import index
from swearch import utils


class SearchMiddleware(object):
    """Search API Middleware."""

    def __init__(self, app, conf, *args, **kwargs):
        self.logger = swift_utils.get_logger(conf, log_route='swearch')

        self.app = app
        # path used to activate search. ex: /[path]/AUTH_12345
        self.path = conf.get('path', 'search')

        config_file = conf.get('config_file', '/etc/swift/swearch.conf')
        config = swift_utils.readconf(config_file)

        index_config = config.get('index', {})
        self.search_index_name = index_config.get(
            'search_index_name', 'os_default')

        hosts_str = index_config.get('elastic_hosts', '127.0.0.1:9200')
        self.elastic_hosts = [x.strip() for x in hosts_str.split(',')]

    def GET(self, req):
        """Serves a GET to the middleware."""
        try:
            version, account, path = swift_utils.split_path(req.path, 2, 3,
                                                            True)
        except ValueError:
            return swob.HTTPBadRequest(request=req)

        if path:
            path = utils.unicode_unquote(path).rstrip("/")

        self.logger.debug("Searching")

        # Get all of the request variables that we need.
        fmt = req.params.get('format', '').lower()
        accept_header = req.headers.get('Accept', '').lower()

        # Check for Accept header as well
        if fmt == '' and accept_header != '':
            if 'json' in accept_header:
                fmt = 'json'
            elif 'xml' in accept_header:
                fmt = 'xml'

        queries = []
        for key, value in req.str_params.items():
            if key.startswith('q.'):
                val = value.decode("utf-8").strip('*')
                queries.append((key[2:], val))

        query = req.str_params.get('q')
        if query:
            query = query.decode("utf-8").strip('*')
        limit = int(
            req.params.get('limit', 0) or req.params.get('rows', 0)) or 100
        start = int(req.params.get('start', 0) or req.params.get('offset', 0))
        sort = req.params.get('sort', None)

        _type = req.params.get('type', None)

        if _type not in ['object', 'container', None, '']:
            return swob.HTTPBadRequest(request=req)

        field = (req.params.get('field', None)
                 or req.params.get('df', None)
                 or '_all')

        marker = req.params.get('marker', None)

        recursive = req.params.get('recursive', True)
        if type(recursive) is not bool:
            if recursive.lower() in ['false', '0', 'f']:
                recursive = False
            else:
                recursive = True

        srch = index.Searcher(self.elastic_hosts,
                              self.search_index_name,
                              account,
                              logger=self.logger)
        srch.logger = self.logger
        if query:
            srch.add_condition(field, query)
        for f, q in queries:
            if f.startswith("meta-"):
                f = "meta." + f[5:]
            srch.add_condition(f, q)
        srch.path = path
        srch.recursive = recursive
        srch.type = _type
        srch.sort = sort
        srch.limit = limit
        srch.start = start
        srch.marker = marker

        try:
            results = srch.execute()
        except socket.timeout:
            return swob.HTTPServiceUnavailable(req=req)

        self.logger.debug(results)

        result_list = []
        for item in results:
            t = index.filter_result_props(item)
            result_list.append(t)

        headers = [
            ('X-Search-Items-Count', len(result_list)),
            ('X-Search-Items-Total', results.total),
            ('X-Search-Items-Offset', start),
        ]

        if fmt == 'json':
            headers.append(('Content-Type', 'application/json; charset=utf-8'))
            return swob.Response(request=req, body=json.dumps(result_list),
                                 headers=headers)
        elif fmt == 'xml':
            headers.append(('Content-Type', 'application/xml; charset=utf-8'))
            output_list = [
                '<?xml version="1.0" encoding="UTF-8"?>', '<results>']
            for res in result_list:
                item = '<object>'
                for key, val in res.iteritems():
                    item += '<%s>%s</%s>' % (
                        key, saxutils.escape(str(val)), key)
                item += '</object>'
                output_list.append(item)
            output_list.append('</results>')
            res_body = '\n'.join(output_list)
            return swob.Response(request=req, body=res_body, headers=headers)
        else:
            headers.append(('Content-Type', 'text/plain'))
            res_body = ''
            for res in result_list:
                for key, val in res.iteritems():
                    res_body += str(key) + ': ' + str(val) + '\n'
                res_body += '\n'
            return swob.Response(request=req, body=res_body, headers=headers)

    def __call__(self, env, start_response):
        req = swob.Request(env)
        is_search = False
        if req.headers.get('X-Context', None) == 'search':
            is_search = True
        else:
            try:
                version, account, path = swift_utils.split_path(req.path, 2, 3,
                                                                True)
                if version.lower() == self.path:
                    is_search = True
            except ValueError:
                return self.app(env, start_response)

        if is_search:
            try:
                if 'swift.authorize' in req.environ:
                    auth_response = req.environ['swift.authorize'](req)
                    if auth_response:
                        return auth_response
                else:  # This is bad, very bad.
                    return swob.HTTPForbidden(request=req)
                return self.GET(req)(env, start_response)
            except Exception:
                traceback.print_exc(file=sys.stdout)
                return swob.HTTPInternalServerError(request=req)
        return self.app(env, start_response)


def filter_factory(global_conf, **local_conf):
    """Filter factory for search filter middleware."""
    conf = global_conf.copy()
    conf.update(local_conf)

    def search_filter(app):
        """Returns search middleware filter instance."""
        return SearchMiddleware(app, conf)
    return search_filter
