"""
    Index module. Provides classes for indexing and searching the index.
"""
import hashlib
import logging
import os

import pyes

from swearch import utils


SORT_WHITELIST = ['dir', 'container', 'name', 'path', 'type']
RESULT_WHITELIST = ['name', 'object', 'container', 'content_type', 'type',
                    'meta']


class UpdateHandler(object):
    def after_POST(self, req):
        """Performed on updates."""
        pass

    def after_PUT(self, req):
        """Performed on creates."""
        pass

    def after_DELETE(self, req):
        """Performed on deletes."""
        pass


class ContainerUpdateHandler(UpdateHandler):
    def __init__(self, indexer, account, name,
                 index='os_default', app=None, logger=None):
        self.indexer = indexer
        self.account = account
        self.name = name
        self.app = app
        self.index_name = index
        self.logger = logger or logging.getLogger(__name__)

    def parse_props(self, headers):
        path = self.name
        props = {
            'account': self.account,
            'container': self.name,
            'path': path,
            'dir': os.path.dirname(path),
            'name': self.name,
            'analyzed_name': self.name,
            'type': 'container',
            'meta': {},
        }

        for name, value in headers.iteritems():
            # Container Meta
            if name.startswith('x-container-meta-'):
                if value == '':
                    try:
                        del props['meta'][name[17:].lower()]
                    except KeyError:
                        pass
                else:

                    metaname = unicode(name[17:].lower(), errors='replace')
                    props['meta'][metaname] = unicode(value, errors='replace')
            elif name == 'x-container-read':
                props['read'] = value
            elif name == 'x-container-write':
                props['write'] = value

        _id = get_index_id(self.account, self.name)
        return _id, props

    def index(self, _id, props, block=False):
        self.indexer.index_doc(self.index_name, _id, props, block=block)

    def update(self, req):
        path = utils.unicode_quote('/v1/%s/%s' % (self.account, self.name))
        head_req = utils.make_request(req.environ, 'HEAD', path)
        resp = head_req.get_response(self.app)
        headers = dict((k.lower(), v) for k, v in resp.headers.iteritems())
        _id, props = self.parse_props(headers)
        self.logger.info("Updating Container %s/%s" %
                         (self.account, self.name))
        return self.index(_id, props)

    def after_POST(self, req):
        """Performed on updates."""
        return self.update(req)

    def after_PUT(self, req):
        """Performed on creates."""
        return self.update(req)

    def after_COPY(self, req):
        """Performed on copies."""
        return self.update(req)

    def after_DELETE(self, req):
        """Performed on deletes."""
        self.logger.info("Deleting Container %s/%s" %
                         (self.account, self.name))
        _id = get_index_id(self.account, self.name)
        return self.indexer.remove_doc(self.index_name, _id)


class ObjectUpdateHandler(UpdateHandler):
    def __init__(self, indexer, account, container, name,
                 index='os_default', app=None, logger=None):
        self.indexer = indexer
        self.account = account
        self.container = container
        self.name = name
        self.app = app
        self.index_name = index
        self.logger = logger or logging.getLogger(__name__)

    def parse_props(self, headers):
        path = "%s/%s" % (self.container, self.name)
        props = {
            'account': self.account,
            'container': self.container,
            'path': path,
            'dir': os.path.dirname(path),
            'name': os.path.basename(self.name),
            'analyzed_name': self.name,
            'object': self.name,
            'type': 'object',
            'hash': headers.get('etag', None),
            'content_type': headers.get(
                'content-type', 'application/octet-stream'),
            'meta': {},
        }

        for name, value in headers.iteritems():
            # Object Meta
            if name.startswith('x-object-meta-'):
                if value == '':
                    try:
                        del props['meta'][name[14:].lower()]
                    except KeyError:
                        pass
                else:
                    metaname = unicode(name[14:].lower(), errors='replace')
                    props['meta'][metaname] = unicode(value, errors='replace')

        _id = get_index_id(self.account, self.container, self.name)
        return _id, props

    def index(self, _id, props, block=False):
        self.indexer.index_doc(self.index_name, _id, props, block=block)

    def update(self, req):
        path = utils.unicode_quote('/v1/%s/%s/%s' %
                                   (self.account, self.container, self.name))
        head_req = utils.make_request(req.environ, 'HEAD', path)
        resp = head_req.get_response(self.app)
        headers = dict((k.lower(), v) for k, v in resp.headers.iteritems())

        _id, props = self.parse_props(headers)
        self.logger.info("Updating Object %s/%s/%s" %
                         (self.account, self.container, self.name))
        return self.index(_id, props)

    def after_POST(self, req):
        """Performed on updates."""
        return self.update(req)

    def after_PUT(self, req):
        """Performed on creates."""
        return self.update(req)

    def after_COPY(self, req):
        """Performed on copies."""
        return self.update(req)

    def after_DELETE(self, req):
        """Performed on deletes."""
        _id = get_index_id(self.account, self.container, self.name)
        self.logger.info("Deleting Object %s/%s/%s" %
                         (self.account, self.container, self.name))
        return self.indexer.remove_doc(self.index_name, _id)


def filter_result_props(props):
    filtered_props = {}
    for key, val in props.iteritems():
        if not key.startswith('_'):
            filtered_props[key] = val
    filtered_props = dict((key, value) for key, value
                          in props.iteritems()
                          if key in RESULT_WHITELIST)

    if props.get('meta'):
        for key, val in props['meta'].iteritems():
            filtered_props["meta_%s" % key] = val
        del props['meta']
    if 'analyzed_name' in props:
        filtered_props['name'] = props['analyzed_name']
    return filtered_props


def get_index_id(account, container=None, obj=None):
    _id = account
    if container:
        _id = "{0}:{1}".format(_id, utils.unicode_encode(container))
    if obj:
        _id = "{0}:{1}".format(_id, utils.unicode_encode(obj))
    return hashlib.sha1(_id).hexdigest()


def get_search_client(config):
    hosts_str = config.get('elastic_hosts', '127.0.0.1:9200')
    elastic_hosts = [x.strip() for x in hosts_str.split(',')]
    conn = pyes.ES(elastic_hosts)
    return conn


class Searcher(object):
    def __init__(self, hosts, index, account, logger=None):
        self.account = account
        self.path = None
        self.recursive = True
        self.type = None
        self.conditions = []
        self.sort = None
        self.limit = None
        self.start = None
        self.marker = None

        self.logger = logger or logging.getLogger(__name__)
        self.elastic_hosts = hosts
        self.search_index_name = index

    def add_condition(self, field, query):
        self.conditions.append((field, query))

    def execute(self):
        conn = pyes.ES(self.elastic_hosts)
        queries = []
        filters = []

        filters.append(pyes.TermFilter('account', self.account))
        for field, val in self.conditions:
            if val != '':
                queries.append(pyes.TextQuery(field, val))

        if self.type not in [None, '']:
            queries.append(pyes.TextQuery('type', self.type))

        if self.path not in [None, '']:
            if self.recursive:
                filters.append(pyes.PrefixFilter('path', '%s/' % self.path))
            else:
                queries.append(pyes.TermQuery('dir', self.path))

        if self.marker not in [None, '']:
            filters.append(
                pyes.RangeFilter(pyes.ESRangeOp('name', 'gt', self.marker)))

        q = pyes.MatchAllQuery()
        if len(queries) > 0:
            q = pyes.BoolQuery(queries)

        q = pyes.FilteredQuery(q, pyes.ANDFilter(filters))
        self.logger.info("Running query: %s" % q.serialize())
        results = conn.search(q, self.search_index_name, start=self.start,
                              size=self.limit)
        if self.sort not in [None, '']:
            sort_list = []
            for row in self.sort.split(','):
                sort_data = row.split(' ')
                prefix = ""
                if len(sort_data) > 1 and sort_data[1].lower() == 'desc':
                    prefix = "-"
                if sort_data[0] in SORT_WHITELIST:
                    sort_list.append("{0}{1}".format(prefix, sort_data[0]))
            if sort_list:
                results.order_by(sort_list)
        return results
