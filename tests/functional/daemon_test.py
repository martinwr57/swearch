# -*- coding:utf-8 -*- 59Q
import ConfigParser
import os
import time
import unittest

import object_storage
#Last edited by Willie Martin 02/10/2015
#Added self.time_out and self.sleep_factor to change the delay for timeouts and retry

def get_config():
    config_file = os.environ.get('SWIFT_TEST_CONFIG_FILE',
                                 '/etc/swift/test.conf')
    section = 'func_test'
    config = ConfigParser.ConfigParser()
    config.read(config_file)

    config_dict = {}
    for option in config.options(section):
        config_dict[option] = config.get(section, option)

    return config_dict

config = get_config()


def try_until_timeout(timeout, f, *args, **kwargs):
    start = time.time()
    while True:
        try:
            f(*args, **kwargs)
        except (AssertionError, KeyError, IndexError):
            end = time.time()
            if end - start > timeout:
                raise
            else:
                time.sleep(2)
        else:
            break


class IndexTest(unittest.TestCase):

    def setUp(self):
        for key in 'account username password'.split():
            if key not in config:
                raise unittest.SkipTest(
                    "%s not set in the test config file" % key)
        protocol = 'http'
        if config.get('auth_ssl', 'no').lower() in ('yes', 'true', 'on', '1'):
            protocol = 'https'
        host = config.get('auth_host', '127.0.0.1')
        port = config.get('auth_port', '8080')
        auth_prefix = config.get('auth_prefix', '/auth/')

        auth_url = '%s://%s:%s%sv1.0' % (protocol, host, port, auth_prefix)
        username = "%s:%s" % (config['account'], config['username'])
        api_key = config['password']
        self.client = object_storage.get_client(
            username, api_key, auth_url=auth_url)
        self.sleep_factor = int(config.get('sleep_factor', 2))
        self.time_out = int(config.get('time_out',2))
        self.delete_data()

    def tearDown(self):
        self.delete_data()

    def delete_data(self):
        containers = ['index_test', 'index_test2', 'flapping', 'space test']
        for container in containers:
            c = self.client[container]
            try:
                objs = c.objects()
            except object_storage.errors.NotFound:
                objs = []
            for obj in objs:
                try:
                    obj.delete()
                except object_storage.errors.NotFound:
                    pass
            try:
                c.delete(True)
            except object_storage.errors.NotFound:
                pass
        time.sleep(self.time_out)

    def insert_data(self):
        bucket = self.client['index_test'].create()
        bucket2 = self.client['index_test2'].create()

        o = bucket['¡¢¥¬ÈÌÛöê']
        o.content_type = 'text/plain'
        o.send('test')

        o = bucket['object_2']
        o.content_type = 'application/octet-stream'
        o.send('test')

        o = bucket2['object_2']
        o.content_type = 'image/jpeg'
        o.send('test')

    def test_prefix_search(self):
        self.insert_data()
        bucket2 = self.client['index_test2']

        def assert_1():
            res = bucket2.search('obje')
            self.assertEqual(res['count'], 1)
            self.assertEqual(res['total'], 1)
            self.assertEqual(len(res['results']), 1)

            res = self.client.search('ind', type='container')
            self.assertEqual(res['count'], 2)
            self.assertEqual(res['total'], 2)
            self.assertEqual(len(res['results']), 2)

        try_until_timeout(self.sleep_factor * 2, assert_1)

    def test_metadata_search(self):
        object_name = '¡¢¥¬ÈÌÛöê'
        bucket = self.client['index_test'].create()
        o = bucket[object_name]
        o.create()
        time.sleep(self.time_out)
        o.set_metadata({'test-a': 'test', 'test-b': 'test2'})

        def assert_1():
            res = bucket.search(**{'q_test-a': 'test'})
            self.assertEqual(res['count'], 1)
            self.assertEqual(res['total'], 1)
            self.assertEqual(len(res['results']), 1)

            res = bucket.search(**{'q_test-b': 'test2'})
            self.assertEqual(res['count'], 1)
            self.assertEqual(res['total'], 1)
            self.assertEqual(len(res['results']), 1)

            res = bucket.search('test2')
            self.assertEqual(res['count'], 1)
            self.assertEqual(res['total'], 1)
            self.assertEqual(len(res['results']), 1)

        try_until_timeout(self.sleep_factor * 2, assert_1)

    def test_basic_updating(self):
        object_name = '¡¢¥¬ÈÌÛöê'
        bucket = self.client['index_test'].create()
        o = bucket[object_name]
        o.create()
        time.sleep(self.time_out)
        o.set_metadata({'test-a': '1', 'test-b': '1'})

        def assert_1():
            res = bucket.search(object_name)
            self.assertEqual(res['count'], 1)
            self.assertEqual(res['total'], 1)
            self.assertEqual(len(res['results']), 1)
            self.assertEqual(res['results'][0].props['meta'].get('test-a'),
                             '1')
            self.assertEqual(res['results'][0].props['meta'].get('test-b'),
                             '1')

        try_until_timeout(self.sleep_factor * 2, assert_1)

        o.set_metadata({'test-a': '2'})

        def assert_2():
            res = bucket.search(object_name)
            self.assertEqual(res['count'], 1)
            self.assertEqual(res['total'], 1)
            self.assertEqual(len(res['results']), 1)

            self.assertEqual(res['results'][0].props['meta']['test-a'], '2')
            self.assertFalse('test-b' in res['results'][0].props['meta'])

        try_until_timeout(self.sleep_factor * 2, assert_2)

        o.set_metadata({'test-b': '2'})

        def assert_3():
            res = bucket.search(object_name)
            self.assertEqual(res['count'], 1)
            self.assertEqual(res['total'], 1)
            self.assertEqual(len(res['results']), 1)

            self.assertEqual(res['results'][0].props['meta']['test-b'], '2')
            self.assertFalse('test-a' in res['results'][0].props['meta'])

        try_until_timeout(self.sleep_factor * 2, assert_3)

    def test_search_by_container(self):
        self.insert_data()

        def assert_1():
            res = self.client.search('', container='index_test')
            self.assertEqual(res['count'], 2)
            self.assertEqual(res['total'], 2)
            self.assertEqual(len(res['results']), 2)

            res = self.client.search('', container='index_test2')
            self.assertEqual(res['count'], 1)
            self.assertEqual(res['total'], 1)
            self.assertEqual(len(res['results']), 1)

        try_until_timeout(self.sleep_factor * 2, assert_1)

    def test_search_by_container_space_extension(self):
        bucket = self.client['space test'].create()
        o = bucket['test-image.vhd']
        o.content_type = 'image/vhd'
        o.send('test')

        def assert_1():
            res = self.client.search('.vhd', container='space test')
            self.assertEqual(res['count'], 1)
            self.assertEqual(res['total'], 1)
            self.assertEqual(len(res['results']), 1)

        try_until_timeout(self.sleep_factor * 2, assert_1)

    def test_search_by_path(self):
        self.insert_data()

        def assert_1():
            res = self.client['index_test'].search('')
            print res
            self.assertEqual(res['count'], 2)
            self.assertEqual(res['total'], 2)
            self.assertEqual(len(res['results']), 2)

            res = self.client['index_test2'].search('')
            self.assertEqual(res['count'], 1)
            self.assertEqual(res['total'], 1)
            self.assertEqual(len(res['results']), 1)

        try_until_timeout(self.sleep_factor * 2, assert_1)

    def test_search_by_content_type(self):
        self.insert_data()

        def assert_1():
            res = self.client['index_test2'].search('', q_content_type='image')
            self.assertEqual(res['count'], 1)
            self.assertEqual(res['total'], 1)
            self.assertEqual(len(res['results']), 1)

            res = self.client['index_test2'].search('', q_content_type='jpeg')
            self.assertEqual(res['count'], 1)
            self.assertEqual(res['total'], 1)
            self.assertEqual(len(res['results']), 1)

            res = self.client['index_test'].search(
                '', q_content_type='octet-stream')
            self.assertEqual(res['count'], 1)
            self.assertEqual(res['total'], 1)
            self.assertEqual(len(res['results']), 1)

        try_until_timeout(self.sleep_factor * 2, assert_1)

    def test_basic_indexing(self):
        self.insert_data()
        bucket = self.client['index_test']
        object_name = '¡¢¥¬ÈÌÛöê'
        object_name2 = 'object_2'

        def assert_1():
            # Check indexing of object
            res = bucket.search(object_name)
            print object_name
            print res
            self.assertEqual(res['count'], 1)
            self.assertEqual(res['total'], 1)
            self.assertEqual(len(res['results']), 1)
            self.assertEqual(
                res['results'][0].name.encode("utf-8"), object_name)
            self.assertEqual(
                res['results'][0].props['content_type'], 'text/plain')

            # Check indexing of object2
            res = bucket.search(q_name=object_name2)
            self.assertEqual(res['count'], 1)
            self.assertEqual(res['total'], 1)
            self.assertEqual(len(res['results']), 1)
            self.assertEqual(res['results'][0].name, object_name2)
            self.assertEqual(
                res['results'][0].props['content_type'],
                'application/octet-stream')

            # Check indexing of container
            res1 = self.client.search('index', type='container')
            self.assertEqual(res1['count'], 2)
            self.assertEqual(res1['total'], 2)
            self.assertEqual(len(res1['results']), 2)
            self.assertEqual(
                1,
                len(filter(lambda r: r.name == 'index_test', res1['results'])))
            self.assertEqual(
                1,
                len(filter(
                    lambda r: r.name == 'index_test2', res1['results'])))
        try_until_timeout(self.sleep_factor * 2, assert_1)

        # Delete object and container
        bucket[object_name].delete()
        bucket[object_name2].delete()
        bucket.delete(True)

        def assert_2():
            # Check index removal of object
            res3 = bucket.search(object_name)
            self.assertEqual(res3['count'], 0)
            self.assertEqual(res3['total'], 0)
            self.assertEqual(len(res3['results']), 0)

            # Check index removal of object2
            res3 = bucket.search(object_name2)
            self.assertEqual(res3['count'], 0)
            self.assertEqual(res3['total'], 0)
            self.assertEqual(len(res3['results']), 0)

            # Check index removal of container
            res4 = bucket.search('index_test', type='container')
            self.assertEqual(res4['count'], 0)
            self.assertEqual(res4['total'], 0)
            self.assertEqual(len(res4['results']), 0)
        try_until_timeout(self.sleep_factor * 2, assert_2)

    #@unittest.skip("this fails randomly, so skip it for now")
    def test_flapping(self):
        self.insert_data()
        self.delete_data()
        bucket = self.client['flapping']
        bucket.create()
        bucket.delete()

        def assert_1():
            res = self.client.search('flapping', type='container')
            self.assertEqual(res['count'], 0)
            self.assertEqual(res['total'], 0)
            self.assertEqual(len(res['results']), 0)
        try_until_timeout(self.sleep_factor * 2, assert_1)

        bucket.create()
        bucket.delete()
        bucket.create()

        def assert_2():
            res = self.client.search('flapping', type='container')
            self.assertEqual(res['count'], 1)
            self.assertEqual(res['total'], 1)
            self.assertEqual(len(res['results']), 1)

        try_until_timeout(self.sleep_factor * 2, assert_1)
