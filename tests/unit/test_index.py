import mock
import unittest
import os

from swearch import utils
from swift.common import swob



class UpdateHandlerTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass	
    
    def index(self):
		
	index_name = mock.Mock()
        _id = mock.Mock()
        props = mock.Mock()
        block = mock.Mock()
        task_complete = self.indexer.index_doc(self.index_name, _id, props, block=block)
        self.assertTrue(task_complete)
        
    def update(self, req):
		
	account = mock.Mock()
        name = mock.Mock()
        app = mock.Mock()
        conn = mock.Mock()
        
        path = utils.unicode_quote('/v1/%s/%s' % (account, name))
        head_req = utils.make_request(req.environ, 'HEAD', path)
        resp = head_req.get_response(app)
        headers = dict((k.lower(), v) for k, v in resp.headers.iteritems())
        _id, props = self.parse_props(headers)
        self.logger.info("Updating Container %s/%s" %
                         (account, name))
        #return self.index(_id, props)  
        self.assertTrue(self.index(_id, props))  
        
    def updateTest(self):
	    pass
        
    def after_POST(self):
        """Performed on updates."""
	req = mock.Mock()
        self.assertEqual(self.update(req), req )
        pass

    def after_PUT(self):
        """Performed on creates."""
	req = mock.Mock()
        self.assertEqual(self.update(req), req )
        pass

    def after_DELETE(self):
        """Performed on deletes."""
	req = mock.Mock()
        self.assertEqual(self.update(req), req )
        pass

class ContainerUpdateHandlerTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass
        
    def parse_props_test(self):
        name = mock.Mock()
        account = mock.Mock()
        path = name
        props = {
            'account': account,
            'container': name,
            'path': path,
            'dir':path, # os.path.dirname(path),
            'name': name,
            'analyzed_name': name,
            'type': 'container',
            'meta': {},
        }
        self.assertTrue(props)
	pass
	
    def indexTest(self):
	pass
		
    def updateTest(self):
	pass
			

    def after_POST(self):
        """Performed on updates."""
	req = mock.Mock()
        self.assertEqual(self.update(req), req )
        return self.update(req)

    def after_PUT(self):
        """Performed on creates."""
	req = mock.Mock()
        self.assertEqual(self.update(req), req )
        return self.update(req)

    def after_COPY(self):
        """Performed on copies."""
	req = mock.Mock()
        self.assertEqual(self.update(req), req )
        return self.update(req)

    def after_DELETE(self):
        """Performed on deletes."""
	req = mock.Mock()
        self.assertEqual(self.update(req), req )
	
	

class ObjectUpdateHandlerTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass
        
    def parse_props_test(self):
	pass
	
    def indexTest(self):
	pass
		
    def updateTest(self):
	pass
			

    def after_POST(self):
        """Performed on updates."""
	req = mock.Mock()
        self.assertEqual(self.update(req), req )
        return self.update(req)

    def after_PUT(self):
        """Performed on creates."""
	req = mock.Mock()
        self.assertEqual(self.update(req), req )
        return self.update(req)

    def after_COPY(self):
        """Performed on copies."""
	req = mock.Mock()
        self.assertEqual(self.update(req), req )
        return self.update(req)

    def after_DELETE(self):
        """Performed on deletes."""
	req = mock.Mock()
        self.assertEqual(self.update(req), req )
	
"""class SearcherTest(unittest.TestCase):
    



    
    def update(self, req):

        account = mock.Mock()
        name = mock.Mock()
        app = mock.Mock()
        conn = mock.Mock()

        path = utils.unicode_quote('/v1/%s/%s' % (account, name))
        head_req = utils.make_request(req.environ, 'HEAD', path)
        resp = head_req.get_response(app)
        headers = dict((k.lower(), v) for k, v in resp.headers.iteritems())
        _id, props = self.parse_props(headers)
        #return self.index(_id, props)  
        self.assertTrue(self.index(_id, props))

	
	
    def add_condition_test(self, field, query):
	field = mock.Mock()
	query = mock.Mock()
        self.assertEqual(self.update(req), req )
        pass

    def execute_test(self):
	req = mock.Mock()
        self.assertEqual(self.update(req), req )
	pass
		
"""	
