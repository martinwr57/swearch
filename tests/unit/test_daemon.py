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




SELF_INDEX = 'search_index_name:,os_default'
class IndexWorkerTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def index_account_test(self, verify=False):
	"""Lists containers for the account and create backfill tasks."""

        url = mock.Mock()
        headers = mock.Mock()
        auth_token = mock.Mock()
        conn = mock.Mock()
                    
        headers, containers = swiftclient.get_account( url, auth_token, full_listing=True, http_conn=conn)
        self.assertTrue(containers)
        
        
        account = mock.Mock()
        verify = verify
        task_complete = self.indexer.publish_backfill_task('container', account, container['name'], verify=verify)
        self.assertTrue(task_complete)
	
	
	
    #@mock.patch('search_index_name', 'os_default')	
    def index_container_test(self, marker=None, verify=False):
		
	 
        # Index the container up to marker
        url = mock.Mock()
        headers = mock.Mock()
        auth_token = mock.Mock()
        conn = mock.Mock()
                    
        headers, containers = swiftclient.get_account( url, auth_token, full_listing=True, http_conn=conn)
        self.assertTrue(containers)             
                
                
        indexer = mock.Mock()
        account = mock.Mock()
        container = mock.Mock()
        objects = mock.Mock()
        headers = mock.Mock()
        verify = mock.Mock()
        
        
        task_complete = self.indexer.publish_backfill_task('container', account, container, verify=verify)
        self.assertTrue(task_complete)
        
        update_handler = index.ContainerUpdateHandler(indexer, account, container, index=index)
        self.assertTrue(update_handler)    
          
        _id, props = update_handler.parse_props(headers)
        self.assertTrue(_id)    
        
        
        task_complete = self.indexer.publish_backfill_task('object', account, objects, verify=verify)
        self.assertTrue(task_complete)
        
    def index_object_test(self, verify=False):
	"""Retrieve details for object and index it."""


        config_file = config.get('config_file', '/etc/swift/swearch.conf')

        self.queue_config = config.get('queue', {})
        self.index_config = config.get('index', {})
               
        indexer = mock.Mock()
        account = mock.Mock()
        container = mock.Mock()
        objects = mock.Mock()
        headers = mock.Mock()
        verify = mock.Mock()
        
        update_handler = index.ObjectUpdateHandler(indexer, account, container, objects, index=index)
        self.assertTrue(update_handler)    
        
        #_id, props = update_handler.parse_props(headers)

        
        client = index.get_search_client(self.index_config)
        _id = mock.Mock() #, props = update_handler.parse_props(headers)
        q = pyes.TermQuery("_id", _id)
        results = client.search(query=q, indices=[self.index_config.get('search_index_name', 'os_default')])
           
		
	
