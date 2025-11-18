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
    
    @patch('toonpy.adapters.mongo_adapter.MongoClient')
    def test_find_one(self, mock_client_class):
        """Test find_one method"""
        mock_collection = self._create_mock_collection([])
        mock_collection.find_one = Mock(return_value={"_id": ObjectId(), "name": "Alice", "age": 30})
        mock_client_class.return_value = self._create_mock_client(mock_collection)
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        result = adapter.find_one({"name": "Alice"})
        
        self.assertIsInstance(result, str)
        self.assertIn("alice", result.lower())
        mock_collection.find_one.assert_called_once_with({"name": "Alice"}, None)
    
    @patch('toonpy.adapters.mongo_adapter.MongoClient')
    def test_find_one_none(self, mock_client_class):
        """Test find_one with no results"""
        mock_collection = self._create_mock_collection([])
        mock_collection.find_one = Mock(return_value=None)
        mock_client_class.return_value = self._create_mock_client(mock_collection)
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        result = adapter.find_one({"name": "NonExistent"})
        
        self.assertIsInstance(result, str)
    
    @patch('toonpy.adapters.mongo_adapter.MongoClient')
    def test_aggregate(self, mock_client_class):
        """Test aggregate method"""
        mock_collection = self._create_mock_collection([])
        mock_cursor = Mock()
        mock_cursor.__iter__ = Mock(return_value=iter([
            {"_id": "admin", "count": 2},
            {"_id": "user", "count": 3}
        ]))
        mock_collection.aggregate = Mock(return_value=mock_cursor)
        mock_client_class.return_value = self._create_mock_client(mock_collection)
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        pipeline = [{"$group": {"_id": "$role", "count": {"$sum": 1}}}]
        result = adapter.aggregate(pipeline)
        
        self.assertIsInstance(result, str)
        mock_collection.aggregate.assert_called_once_with(pipeline)
    
    @patch('toonpy.adapters.mongo_adapter.MongoClient')
    def test_count_documents(self, mock_client_class):
        """Test count_documents method"""
        mock_collection = self._create_mock_collection([])
        mock_collection.count_documents = Mock(return_value=5)
        mock_client_class.return_value = self._create_mock_client(mock_collection)
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        result = adapter.count_documents({"role": "admin"})
        
        self.assertEqual(result, 5)
        mock_collection.count_documents.assert_called_once_with({"role": "admin"})
    
    @patch('toonpy.adapters.mongo_adapter.MongoClient')
    def test_distinct(self, mock_client_class):
        """Test distinct method"""
        mock_collection = self._create_mock_collection([])
        mock_collection.distinct = Mock(return_value=["admin", "user", "guest"])
        mock_client_class.return_value = self._create_mock_client(mock_collection)
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        result = adapter.distinct("role")
        
        self.assertIsInstance(result, str)
        mock_collection.distinct.assert_called_once_with("role", {})
    
    @patch('toonpy.adapters.mongo_adapter.MongoClient')
    def test_insert_one_from_toon(self, mock_client_class):
        """Test insert_one_from_toon method"""
        from toonpy.core.converter import to_toon
        
        mock_collection = self._create_mock_collection([])
        mock_result = Mock()
        mock_result.inserted_id = ObjectId()
        mock_result.acknowledged = True
        mock_collection.insert_one = Mock(return_value=mock_result)
        mock_client_class.return_value = self._create_mock_client(mock_collection)
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        
        document = {"name": "Test User", "age": 25}
        toon_string = to_toon([document])
        result = adapter.insert_one_from_toon(toon_string)
        
        self.assertIsInstance(result, str)
        mock_collection.insert_one.assert_called_once()
    
    @patch('toonpy.adapters.mongo_adapter.MongoClient')
    def test_insert_many_from_toon(self, mock_client_class):
        """Test insert_many_from_toon method"""
        from toonpy.core.converter import to_toon
        
        mock_collection = self._create_mock_collection([])
        mock_result = Mock()
        mock_result.inserted_ids = [ObjectId(), ObjectId()]
        mock_result.acknowledged = True
        mock_collection.insert_many = Mock(return_value=mock_result)
        mock_client_class.return_value = self._create_mock_client(mock_collection)
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        
        documents = [
            {"name": "User 1", "age": 25},
            {"name": "User 2", "age": 30}
        ]
        toon_string = to_toon(documents)
        result = adapter.insert_many_from_toon(toon_string)
        
        self.assertIsInstance(result, str)
        mock_collection.insert_many.assert_called_once()
    
    @patch('toonpy.adapters.mongo_adapter.MongoClient')
    def test_update_one_from_toon(self, mock_client_class):
        """Test update_one_from_toon method"""
        from toonpy.core.converter import to_toon
        
        mock_collection = self._create_mock_collection([])
        mock_result = Mock()
        mock_result.matched_count = 1
        mock_result.modified_count = 1
        mock_result.upserted_id = None
        mock_result.acknowledged = True
        mock_collection.update_one = Mock(return_value=mock_result)
        mock_client_class.return_value = self._create_mock_client(mock_collection)
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        
        update_data = {"age": 31, "status": "active"}
        toon_string = to_toon([update_data])
        result = adapter.update_one_from_toon({"name": "Alice"}, toon_string)
        
        self.assertIsInstance(result, str)
        mock_collection.update_one.assert_called_once()
    
    @patch('toonpy.adapters.mongo_adapter.MongoClient')
    def test_update_many_from_toon(self, mock_client_class):
        """Test update_many_from_toon method"""
        from toonpy.core.converter import to_toon
        
        mock_collection = self._create_mock_collection([])
        mock_result = Mock()
        mock_result.matched_count = 3
        mock_result.modified_count = 3
        mock_result.acknowledged = True
        mock_collection.update_many = Mock(return_value=mock_result)
        mock_client_class.return_value = self._create_mock_client(mock_collection)
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        
        update_data = {"status": "inactive"}
        toon_string = to_toon([update_data])
        result = adapter.update_many_from_toon({"role": "user"}, toon_string)
        
        self.assertIsInstance(result, str)
        mock_collection.update_many.assert_called_once()
    
    @patch('toonpy.adapters.mongo_adapter.MongoClient')
    def test_replace_one_from_toon(self, mock_client_class):
        """Test replace_one_from_toon method"""
        from toonpy.core.converter import to_toon
        
        mock_collection = self._create_mock_collection([])
        mock_result = Mock()
        mock_result.matched_count = 1
        mock_result.modified_count = 1
        mock_result.upserted_id = None
        mock_result.acknowledged = True
        mock_collection.replace_one = Mock(return_value=mock_result)
        mock_client_class.return_value = self._create_mock_client(mock_collection)
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        
        replacement = {"name": "Alice Updated", "age": 32, "role": "admin"}
        toon_string = to_toon([replacement])
        result = adapter.replace_one_from_toon({"name": "Alice"}, toon_string)
        
        self.assertIsInstance(result, str)
        mock_collection.replace_one.assert_called_once()
    
    @patch('toonpy.adapters.mongo_adapter.MongoClient')
    def test_delete_one(self, mock_client_class):
        """Test delete_one method"""
        mock_collection = self._create_mock_collection([])
        mock_result = Mock()
        mock_result.deleted_count = 1
        mock_result.acknowledged = True
        mock_collection.delete_one = Mock(return_value=mock_result)
        mock_client_class.return_value = self._create_mock_client(mock_collection)
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        result = adapter.delete_one({"name": "Test"})
        
        self.assertIsInstance(result, str)
        mock_collection.delete_one.assert_called_once_with({"name": "Test"})
    
    @patch('toonpy.adapters.mongo_adapter.MongoClient')
    def test_delete_many(self, mock_client_class):
        """Test delete_many method"""
        mock_collection = self._create_mock_collection([])
        mock_result = Mock()
        mock_result.deleted_count = 3
        mock_result.acknowledged = True
        mock_collection.delete_many = Mock(return_value=mock_result)
        mock_client_class.return_value = self._create_mock_client(mock_collection)
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        result = adapter.delete_many({"role": "guest"})
        
        self.assertIsInstance(result, str)
        mock_collection.delete_many.assert_called_once_with({"role": "guest"})

    @patch('toonpy.adapters.mongo_adapter.MongoClient')
    def test_insert_and_query_from_toon(self, mock_client_class):
        """Test insert_and_query_from_toon method"""
        from toonpy.core.converter import to_toon
        
        mock_collection = self._create_mock_collection([])
        mock_result = Mock()
        mock_result.inserted_id = ObjectId()
        mock_result.acknowledged = True
        mock_collection.insert_one = Mock(return_value=mock_result)
        
        # Mock find_one to return the inserted document
        inserted_doc = {"_id": mock_result.inserted_id, "name": "Test User", "age": 25}
        mock_collection.find_one = Mock(return_value=inserted_doc)
        
        mock_client_class.return_value = self._create_mock_client(mock_collection)
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        
        document = {"name": "Test User", "age": 25}
        toon_string = to_toon([document])
        result = adapter.insert_and_query_from_toon(toon_string)
        
        self.assertIsInstance(result, str)
        self.assertIn("test user", result.lower())
        mock_collection.insert_one.assert_called_once()
        mock_collection.find_one.assert_called_once()
    
    @patch('toonpy.adapters.mongo_adapter.MongoClient')
    def test_insert_many_and_query_from_toon(self, mock_client_class):
        """Test insert_many_and_query_from_toon method"""
        from toonpy.core.converter import to_toon
        
        mock_collection = self._create_mock_collection([])
        mock_result = Mock()
        mock_result.inserted_ids = [ObjectId(), ObjectId()]
        mock_result.acknowledged = True
        mock_collection.insert_many = Mock(return_value=mock_result)
        
        # Mock find to return inserted documents
        inserted_docs = [
            {"_id": mock_result.inserted_ids[0], "name": "User 1", "age": 25},
            {"_id": mock_result.inserted_ids[1], "name": "User 2", "age": 30}
        ]
        mock_cursor = Mock()
        mock_cursor.__iter__ = Mock(return_value=iter(inserted_docs))
        mock_cursor.limit = Mock(return_value=mock_cursor)
        mock_collection.find = Mock(return_value=mock_cursor)
        
        mock_client_class.return_value = self._create_mock_client(mock_collection)
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        
        documents = [
            {"name": "User 1", "age": 25},
            {"name": "User 2", "age": 30}
        ]
        toon_string = to_toon(documents)
        result = adapter.insert_many_and_query_from_toon(toon_string)
        
        self.assertIsInstance(result, str)
        self.assertIn("user 1", result.lower())
        self.assertIn("user 2", result.lower())
        mock_collection.insert_many.assert_called_once()
        mock_collection.find.assert_called_once()
    
    @patch('toonpy.adapters.mongo_adapter.MongoClient')
    def test_update_and_query_from_toon(self, mock_client_class):
        """Test update_and_query_from_toon method"""
        from toonpy.core.converter import to_toon
        
        mock_collection = self._create_mock_collection([])
        mock_update_result = Mock()
        mock_update_result.matched_count = 1
        mock_update_result.modified_count = 1
        mock_update_result.upserted_id = None
        mock_update_result.acknowledged = True
        mock_collection.update_one = Mock(return_value=mock_update_result)
        
        # Mock find_one to return updated document
        updated_doc = {"_id": ObjectId(), "name": "Alice", "age": 31, "status": "active"}
        mock_collection.find_one = Mock(return_value=updated_doc)
        
        mock_client_class.return_value = self._create_mock_client(mock_collection)
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        
        update_data = {"age": 31, "status": "active"}
        toon_string = to_toon([update_data])
        result = adapter.update_and_query_from_toon({"name": "Alice"}, toon_string)
        
        self.assertIsInstance(result, str)
        self.assertIn("alice", result.lower())
        mock_collection.update_one.assert_called_once()
        mock_collection.find_one.assert_called_once()
    
    @patch('toonpy.adapters.mongo_adapter.MongoClient')
    def test_replace_and_query_from_toon(self, mock_client_class):
        """Test replace_and_query_from_toon method"""
        from toonpy.core.converter import to_toon
        
        mock_collection = self._create_mock_collection([])
        mock_replace_result = Mock()
        mock_replace_result.matched_count = 1
        mock_replace_result.modified_count = 1
        mock_replace_result.upserted_id = None
        mock_replace_result.acknowledged = True
        mock_collection.replace_one = Mock(return_value=mock_replace_result)
        
        # Mock find_one to return replaced document
        replaced_doc = {"_id": ObjectId(), "name": "Alice Updated", "age": 32}
        mock_collection.find_one = Mock(return_value=replaced_doc)
        
        mock_client_class.return_value = self._create_mock_client(mock_collection)
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        
        replacement = {"name": "Alice Updated", "age": 32}
        toon_string = to_toon([replacement])
        result = adapter.replace_and_query_from_toon({"name": "Alice"}, toon_string)
        
        self.assertIsInstance(result, str)
        self.assertIn("alice updated", result.lower())
        mock_collection.replace_one.assert_called_once()
        mock_collection.find_one.assert_called_once()


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
    
    def test_find_one(self):
        """Test find_one integration"""
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        result = adapter.find_one({"role": "admin"})
        
        self.assertIsInstance(result, str)
        self.assertIn("alice", result.lower())
        adapter.close()
    
    def test_find_one_not_found(self):
        """Test find_one with no match"""
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        result = adapter.find_one({"name": "NonExistentUser12345"})
        
        self.assertIsInstance(result, str)
        adapter.close()
    
    def test_aggregate(self):
        """Test aggregate integration"""
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        pipeline = [
            {"$group": {"_id": "$role", "count": {"$sum": 1}}}
        ]
        result = adapter.aggregate(pipeline)
        
        self.assertIsInstance(result, str)
        adapter.close()
    
    def test_count_documents(self):
        """Test count_documents integration"""
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        count = adapter.count_documents({"role": "admin"})
        
        self.assertIsInstance(count, int)
        self.assertGreaterEqual(count, 0)
        adapter.close()
    
    def test_distinct(self):
        """Test distinct integration"""
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        result = adapter.distinct("role")
        
        self.assertIsInstance(result, str)
        adapter.close()
    
    def test_insert_one_from_toon(self):
        """Test insert_one_from_toon integration"""
        from toonpy.core.converter import to_toon
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        
        # Insert test document
        document = {
            "name": "Test Insert User",
            "age": 28,
            "role": "test",
            "email": "testinsert@example.com"
        }
        toon_string = to_toon([document])
        result = adapter.insert_one_from_toon(toon_string)
        
        self.assertIsInstance(result, str)
        self.assertIn("inserted_id", result.lower())
        
        # Clean up
        adapter.delete_one({"email": "testinsert@example.com"})
        adapter.close()
    
    def test_insert_many_from_toon(self):
        """Test insert_many_from_toon integration"""
        from toonpy.core.converter import to_toon
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        
        # Insert test documents
        documents = [
            {"name": "Test User 1", "age": 25, "role": "test", "email": "test1@example.com"},
            {"name": "Test User 2", "age": 26, "role": "test", "email": "test2@example.com"}
        ]
        toon_string = to_toon(documents)
        result = adapter.insert_many_from_toon(toon_string)
        
        self.assertIsInstance(result, str)
        self.assertIn("inserted_ids", result.lower())
        
        # Clean up
        adapter.delete_many({"role": "test", "email": {"$in": ["test1@example.com", "test2@example.com"]}})
        adapter.close()
    
    def test_update_one_from_toon(self):
        """Test update_one_from_toon integration"""
        from toonpy.core.converter import to_toon
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        
        # First insert a test document
        test_doc = {"name": "Update Test", "age": 25, "role": "test", "email": "updatetest@example.com"}
        adapter.collection.insert_one(test_doc)
        
        # Update it
        update_data = {"age": 30, "status": "updated"}
        toon_string = to_toon([update_data])
        result = adapter.update_one_from_toon({"email": "updatetest@example.com"}, toon_string)
        
        self.assertIsInstance(result, str)
        self.assertIn("matched_count", result.lower())
        
        # Verify update
        updated = adapter.find_one({"email": "updatetest@example.com"})
        self.assertIn("30", updated)
        
        # Clean up
        adapter.delete_one({"email": "updatetest@example.com"})
        adapter.close()
    
    def test_update_many_from_toon(self):
        """Test update_many_from_toon integration"""
        from toonpy.core.converter import to_toon
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        
        # Insert test documents
        test_docs = [
            {"name": "Batch Update 1", "age": 20, "role": "batch_test", "email": "batch1@example.com"},
            {"name": "Batch Update 2", "age": 21, "role": "batch_test", "email": "batch2@example.com"}
        ]
        adapter.collection.insert_many(test_docs)
        
        # Update them
        update_data = {"status": "batch_updated"}
        toon_string = to_toon([update_data])
        result = adapter.update_many_from_toon({"role": "batch_test"}, toon_string)
        
        self.assertIsInstance(result, str)
        self.assertIn("matched_count", result.lower())
        
        # Clean up
        adapter.delete_many({"role": "batch_test"})
        adapter.close()
    
    def test_replace_one_from_toon(self):
        """Test replace_one_from_toon integration"""
        from toonpy.core.converter import to_toon
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        
        # Insert test document
        test_doc = {"name": "Replace Test", "age": 25, "role": "test", "email": "replacetest@example.com"}
        adapter.collection.insert_one(test_doc)
        
        # Replace it
        replacement = {"name": "Replaced User", "age": 35, "email": "replacetest@example.com"}
        toon_string = to_toon([replacement])
        result = adapter.replace_one_from_toon({"email": "replacetest@example.com"}, toon_string)
        
        self.assertIsInstance(result, str)
        self.assertIn("matched_count", result.lower())
        
        # Verify replacement
        replaced = adapter.find_one({"email": "replacetest@example.com"})
        self.assertIn("replaced", replaced.lower())
        
        # Clean up
        adapter.delete_one({"email": "replacetest@example.com"})
        adapter.close()
    
    def test_delete_one(self):
        """Test delete_one integration"""
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        
        # Insert test document
        test_doc = {"name": "Delete Test", "age": 25, "role": "test", "email": "deletetest@example.com"}
        adapter.collection.insert_one(test_doc)
        
        # Delete it
        result = adapter.delete_one({"email": "deletetest@example.com"})
        
        self.assertIsInstance(result, str)
        self.assertIn("deleted_count", result.lower())
        
        # Verify deletion
        deleted = adapter.find_one({"email": "deletetest@example.com"})
        # Should return empty TOON
        self.assertIsInstance(deleted, str)
        
        adapter.close()
    
    def test_delete_many(self):
        """Test delete_many integration"""
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        
        # Insert test documents
        test_docs = [
            {"name": "Batch Delete 1", "age": 20, "role": "delete_test", "email": "batchdel1@example.com"},
            {"name": "Batch Delete 2", "age": 21, "role": "delete_test", "email": "batchdel2@example.com"}
        ]
        adapter.collection.insert_many(test_docs)
        
        # Delete them
        result = adapter.delete_many({"role": "delete_test"})
        
        self.assertIsInstance(result, str)
        self.assertIn("deleted_count", result.lower())
        
        # Verify deletion
        remaining = adapter.count_documents({"role": "delete_test"})
        self.assertEqual(remaining, 0)
        
        adapter.close()
    
    def test_insert_and_query_from_toon(self):
        """Test insert_and_query_from_toon integration"""
        from toonpy.core.converter import to_toon
        import time
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        
        # Use unique email to avoid conflicts
        unique_email = f"roundtrip{int(time.time())}@example.com"
        
        # Clean up any existing test data first
        adapter.delete_many({"email": unique_email})
        
        # Insert and query back
        document = {
            "name": "Round Trip Test",
            "email": unique_email,
            "age": 28,
            "role": "test"
        }
        toon_string = to_toon([document])
        result = adapter.insert_and_query_from_toon(toon_string)
        
        self.assertIsInstance(result, str)
        self.assertIn("round trip test", result.lower())
        self.assertIn(unique_email, result.lower())
        self.assertIn("28", result)
        
        # Verify it's actual document data (not just operation result)
        from toonpy.core.converter import from_toon
        result_data = from_toon(result)
        self.assertGreater(len(result_data), 0)
        self.assertIn("_id", result_data[0])
        self.assertEqual(result_data[0]["email"], unique_email)
        
        # Clean up
        adapter.delete_one({"email": unique_email})
        adapter.close()
    
    def test_insert_many_and_query_from_toon(self):
        """Test insert_many_and_query_from_toon integration"""
        from toonpy.core.converter import to_toon
        import time
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        
        # Use unique emails to avoid conflicts
        timestamp = int(time.time())
        email1 = f"roundtrip1{timestamp}@example.com"
        email2 = f"roundtrip2{timestamp}@example.com"
        
        # Clean up any existing test data first
        adapter.delete_many({"email": {"$in": [email1, email2]}})
        
        # Insert multiple and query back
        documents = [
            {"name": "Round Trip 1", "email": email1, "age": 25, "role": "test"},
            {"name": "Round Trip 2", "email": email2, "age": 26, "role": "test"}
        ]
        toon_string = to_toon(documents)
        result = adapter.insert_many_and_query_from_toon(toon_string)
        
        self.assertIsInstance(result, str)
        self.assertIn("round trip 1", result.lower())
        self.assertIn("round trip 2", result.lower())
        
        # Verify it's actual document data
        from toonpy.core.converter import from_toon
        result_data = from_toon(result)
        self.assertEqual(len(result_data), 2)
        self.assertIn("_id", result_data[0])
        self.assertIn("_id", result_data[1])
        
        # Clean up
        adapter.delete_many({"email": {"$in": [email1, email2]}})
        adapter.close()
    
    def test_update_and_query_from_toon(self):
        """Test update_and_query_from_toon integration"""
        from toonpy.core.converter import to_toon
        import time
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        
        # Use unique email to avoid conflicts
        unique_email = f"updateroundtrip{int(time.time())}@example.com"
        
        # Clean up any existing test data first
        adapter.delete_many({"email": unique_email})
        
        # First insert a test document
        test_doc = {"name": "Update Round Trip", "email": unique_email, "age": 25, "role": "test"}
        adapter.collection.insert_one(test_doc)
        
        # Update and query back
        update_data = {"age": 35, "status": "updated"}
        toon_string = to_toon([update_data])
        result = adapter.update_and_query_from_toon({"email": unique_email}, toon_string)
        
        self.assertIsInstance(result, str)
        self.assertIn("update round trip", result.lower())
        
        # Verify update was applied
        from toonpy.core.converter import from_toon
        result_data = from_toon(result)
        self.assertGreater(len(result_data), 0)
        self.assertEqual(result_data[0]["age"], 35)
        self.assertEqual(result_data[0]["status"], "updated")
        
        # Clean up
        adapter.delete_one({"email": unique_email})
        adapter.close()
    
    def test_replace_and_query_from_toon(self):
        """Test replace_and_query_from_toon integration"""
        from toonpy.core.converter import to_toon
        import time
        
        adapter = MongoAdapter(
            connection_string=MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        
        # Use unique email to avoid conflicts
        unique_email = f"replaceroundtrip{int(time.time())}@example.com"
        
        # Clean up any existing test data first
        adapter.delete_many({"email": unique_email})
        
        # First insert a test document
        test_doc = {"name": "Replace Round Trip", "email": unique_email, "age": 25, "role": "test"}
        adapter.collection.insert_one(test_doc)
        
        # Replace and query back
        replacement = {"name": "Replaced User", "age": 40, "email": unique_email}
        toon_string = to_toon([replacement])
        result = adapter.replace_and_query_from_toon({"email": unique_email}, toon_string)
        
        self.assertIsInstance(result, str)
        self.assertIn("replaced user", result.lower())
        
        # Verify replacement (old fields should be gone, new fields present)
        from toonpy.core.converter import from_toon
        result_data = from_toon(result)
        self.assertGreater(len(result_data), 0)
        self.assertEqual(result_data[0]["name"], "Replaced User")
        self.assertEqual(result_data[0]["age"], 40)
        # role field should be gone (replaced, not updated)
        self.assertNotIn("role", result_data[0])
        
        # Clean up
        adapter.delete_one({"email": unique_email})
        adapter.close()


if __name__ == "__main__":
    unittest.main()

