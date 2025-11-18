"""
Tests for MongoAdapter
Includes both unit tests (with mocks) and integration tests (with Docker MongoDB)
"""
import unittest
from unittest.mock import Mock, patch, MagicMock
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime, date
import json

from toonpy import MongoAdapter

# Connection settings for Docker MongoDB instance
MONGO_CONN_STRING = "mongodb://localhost:27017"
MONGO_DATABASE = "testdb"


class TestMongoAdapterUnit(unittest.TestCase):
    """Unit tests with mocked connections"""
    
    def test_init_with_collection(self):
        """Test initialization with existing collection object"""
        mock_collection = Mock()
        mock_collection.database.client = Mock()
        
        adapter = MongoAdapter(collection=mock_collection)
        self.assertEqual(adapter.collection, mock_collection)
        self.assertFalse(adapter.own_connection)
    
    @patch('toonpy.adapters.mongo_adapter.MongoClient')
    def test_init_with_connection_string(self, mock_client_class):
        """Test initialization with connection string"""
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_client_class.return_value = mock_client
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        self.assertEqual(adapter.collection, mock_collection)
        self.assertTrue(adapter.own_connection)
        adapter.close()
    
    def test_init_invalid_configuration(self):
        """Test initialization with invalid configuration"""
        # Missing required parameters
        with self.assertRaises(ValueError):
            MongoAdapter(connection_string=MONGO_CONN_STRING)
        
        with self.assertRaises(ValueError):
            MongoAdapter(connection_string=MONGO_CONN_STRING, database=MONGO_DATABASE)
        
        with self.assertRaises(ValueError):
            MongoAdapter(database=MONGO_DATABASE, collection_name="users")
    
    @patch('toonpy.adapters.mongo_adapter.MongoClient')
    def test_find_basic(self, mock_client_class):
        """Test basic find query"""
        mock_collection = self._create_mock_collection([
            {"_id": ObjectId(), "name": "Alice", "age": 30},
            {"_id": ObjectId(), "name": "Bob", "age": 25}
        ])
        mock_client_class.return_value = self._create_mock_client(mock_collection)
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        result = adapter.find()
        
        self.assertIsInstance(result, str)
        self.assertIn("alice", result.lower())
        self.assertIn("bob", result.lower())
        # Don't close - collection is mocked, will cause issues
    
    @patch('toonpy.adapters.mongo_adapter.MongoClient')
    def test_find_with_query(self, mock_client_class):
        """Test find with query filter"""
        mock_collection = self._create_mock_collection([
            {"_id": ObjectId(), "name": "Alice", "age": 30, "role": "admin"}
        ])
        mock_client_class.return_value = self._create_mock_client(mock_collection)
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        result = adapter.find({"role": "admin"})
        
        self.assertIsInstance(result, str)
        self.assertIn("alice", result.lower())
    
    @patch('toonpy.adapters.mongo_adapter.MongoClient')
    def test_find_with_projection(self, mock_client_class):
        """Test find with projection"""
        mock_collection = self._create_mock_collection([
            {"_id": ObjectId(), "name": "Alice", "age": 30}
        ])
        mock_client_class.return_value = self._create_mock_client(mock_collection)
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        result = adapter.find(projection={"name": 1, "age": 1, "_id": 0})
        
        self.assertIsInstance(result, str)
    
    @patch('toonpy.adapters.mongo_adapter.MongoClient')
    def test_query_with_dict(self, mock_client_class):
        """Test query method with dictionary"""
        mock_collection = self._create_mock_collection([
            {"_id": ObjectId(), "name": "Alice", "age": 30}
        ])
        mock_client_class.return_value = self._create_mock_client(mock_collection)
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        result = adapter.query({"age": {"$gt": 25}})
        
        self.assertIsInstance(result, str)
    
    @patch('toonpy.adapters.mongo_adapter.MongoClient')
    def test_query_with_json_string(self, mock_client_class):
        """Test query method with JSON string"""
        mock_collection = self._create_mock_collection([
            {"_id": ObjectId(), "name": "Alice", "age": 30}
        ])
        mock_client_class.return_value = self._create_mock_client(mock_collection)
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        result = adapter.query('{"age": {"$gt": 25}}')
        
        self.assertIsInstance(result, str)
    
    @patch('toonpy.adapters.mongo_adapter.MongoClient')
    def test_query_empty(self, mock_client_class):
        """Test query with no results"""
        mock_collection = self._create_mock_collection([])
        mock_client_class.return_value = self._create_mock_client(mock_collection)
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        result = adapter.query({"age": {"$gt": 100}})
        
        self.assertIsInstance(result, str)
    
    @patch('toonpy.adapters.mongo_adapter.MongoClient')
    def test_clean_mongo_docs_objectid(self, mock_client_class):
        """Test cleaning ObjectId to string"""
        obj_id = ObjectId()
        mock_collection = self._create_mock_collection([
            {"_id": obj_id, "name": "Alice"}
        ])
        mock_client_class.return_value = self._create_mock_client(mock_collection)
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        result = adapter.find()
        
        # ObjectId should be converted to string in TOON output
        self.assertIsInstance(result, str)
        self.assertIn(str(obj_id), result)
    
    @patch('toonpy.adapters.mongo_adapter.MongoClient')
    def test_clean_mongo_docs_datetime(self, mock_client_class):
        """Test cleaning datetime to ISO format"""
        test_date = datetime(2024, 1, 15, 10, 30, 0)
        mock_collection = self._create_mock_collection([
            {"_id": ObjectId(), "name": "Alice", "created_at": test_date}
        ])
        mock_client_class.return_value = self._create_mock_client(mock_collection)
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        result = adapter.find()
        
        # Datetime should be converted to ISO format
        self.assertIsInstance(result, str)
        self.assertIn("2024-01-15", result)
    
    @patch('toonpy.adapters.mongo_adapter.MongoClient')
    def test_close_own_connection(self, mock_client_class):
        """Test closing connection when adapter owns it"""
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_collection.database.client = mock_client
        mock_client_class.return_value = mock_client
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        adapter.close()
        
        mock_client.close.assert_called_once()
    
    def test_close_external_collection(self):
        """Test that close doesn't affect external collection"""
        mock_collection = Mock()
        mock_collection.database.client = Mock()
        
        adapter = MongoAdapter(collection=mock_collection)
        adapter.close()
        
        # Should not raise error or close external connection
        self.assertIsNotNone(adapter.collection)
    
    def _create_mock_collection(self, documents):
        """Helper to create a mock collection with documents"""
        mock_collection = Mock()
        mock_cursor = Mock()
        mock_cursor.__iter__ = Mock(return_value=iter(documents))
        mock_cursor.__next__ = Mock(side_effect=iter(documents) if documents else StopIteration)
        mock_collection.find.return_value = mock_cursor
        return mock_collection
    
    def _create_mock_client(self, mock_collection):
        """Helper to create a mock client with collection"""
        mock_client = Mock()
        mock_db = Mock()
        mock_db.__getitem__ = Mock(return_value=mock_collection)
        mock_client.__getitem__ = Mock(return_value=mock_db)
        mock_collection.database.client = mock_client
        return mock_client


class TestMongoAdapterIntegration(unittest.TestCase):
    """Integration tests with Docker MongoDB instance"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test database before all tests"""
        try:
            client = MongoClient(MONGO_CONN_STRING)
            db = client[MONGO_DATABASE]
            
            # Verify connection
            client.admin.command('ping')
            print("✓ Connected to MongoDB")
            
            # Ensure test data exists
            if db.users.count_documents({}) == 0:
                print("Setting up test data...")
                import sys
                import os
                sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))
                from setup_mongodb import setup_mongodb
                setup_mongodb()
            
            client.close()
        except Exception as e:
            print(f"⚠ Warning: Could not connect to MongoDB: {e}")
            print("  Integration tests will be skipped")
            cls.skip_integration = True
        else:
            cls.skip_integration = False
    
    def setUp(self):
        """Set up before each test"""
        if self.skip_integration:
            self.skipTest("MongoDB not available")
    
    def test_connection(self):
        """Test connecting to MongoDB"""
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        self.assertIsNotNone(adapter.collection)
        adapter.close()
    
    def test_find_all(self):
        """Test finding all documents"""
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        result = adapter.find()
        
        self.assertIsInstance(result, str)
        self.assertIn("alice", result.lower())
        adapter.close()
    
    def test_find_with_query(self):
        """Test find with query filter"""
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        result = adapter.find({"role": "admin"})
        
        self.assertIsInstance(result, str)
        self.assertIn("alice", result.lower())
        adapter.close()
    
    def test_find_with_complex_query(self):
        """Test find with complex query"""
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        result = adapter.find({"age": {"$gt": 25}, "is_active": True})
        
        self.assertIsInstance(result, str)
        adapter.close()
    
    def test_find_with_projection(self):
        """Test find with projection"""
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        result = adapter.find(
            {"role": "admin"},
            projection={"name": 1, "email": 1, "_id": 0}
        )
        
        self.assertIsInstance(result, str)
        self.assertIn("alice", result.lower())
        adapter.close()
    
    def test_query_method(self):
        """Test query method (abstract method implementation)"""
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        result = adapter.query({"age": {"$gte": 30}})
        
        self.assertIsInstance(result, str)
        adapter.close()
    
    def test_query_with_json_string(self):
        """Test query with JSON string"""
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        result = adapter.query('{"role": "user"}')
        
        self.assertIsInstance(result, str)
        adapter.close()
    
    def test_nested_documents(self):
        """Test querying nested documents"""
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        result = adapter.find({"address.city": "New York"})
        
        self.assertIsInstance(result, str)
        self.assertIn("alice", result.lower())
        adapter.close()
    
    def test_array_fields(self):
        """Test querying documents with array fields"""
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        result = adapter.find({"tags": "premium"})
        
        self.assertIsInstance(result, str)
        adapter.close()
    
    def test_products_collection(self):
        """Test querying products collection"""
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="products"
        )
        result = adapter.find({"category": "electronics"})
        
        self.assertIsInstance(result, str)
        self.assertIn("laptop", result.lower())
        adapter.close()
    
    def test_orders_collection(self):
        """Test querying orders collection"""
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="orders"
        )
        result = adapter.find({"status": "completed"})
        
        self.assertIsInstance(result, str)
        adapter.close()
    
    def test_empty_result(self):
        """Test query that returns no results"""
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        result = adapter.find({"age": {"$gt": 100}})
        
        self.assertIsInstance(result, str)
        adapter.close()
    
    def test_existing_collection_object(self):
        """Test using existing collection object"""
        client = MongoClient(MONGO_CONN_STRING)
        collection = client[MONGO_DATABASE]["users"]
        
        adapter = MongoAdapter(collection=collection)
        result = adapter.find()
        
        self.assertIsInstance(result, str)
        # Should not close external connection
        adapter.close()
        # Verify collection still works
        self.assertGreater(collection.count_documents({}), 0)
        client.close()
    
    def test_close(self):
        """Test closing connection"""
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        adapter.close()
        # Should not raise error
        self.assertIsNotNone(adapter.collection)


if __name__ == "__main__":
    unittest.main()

