import mock
import unittest
import os

from swearch import utils
from swift.common import swob
import ConfigParser

import pyes
from swift.common import daemon
from swift.common import utils
import swiftclient

from swearch import index
from swearch import rabbit
from swearch import daemon
from swearch.middleware import indexer






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


class IndexerTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass	

		
    def index_request_test(self):

        config_file = config.get('config_file', '/etc/swift/swearch.conf')

        self.queue_config = config.get('queue', {})
        self.index_config = config.get('index', {})
	indexer = mock.Mock()
        account = mock.Mock()
        container = mock.Mock()
        obj = mock.Mock()
        logger = mock.Mock()
        app = mock.Mock()
	
	update_handler = index.ObjectUpdateHandler(
                indexer,
                account,
                container,
                obj,
                index=self.index_config.get('search_index_name'),
                app=app,
                logger=logger)     
        
        self.assertTrue(update_handler)
             
                
        update_handler = index.ContainerUpdateHandler(
                indexer,
                account,
                container,
                index=self.index_config.get('search_index_name'),
                app=app,
                logger=logger)
        
        self.assertTrue(update_handler)
             
		
        
     
