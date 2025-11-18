"""
Intensive tests for MongoDB adapter cleaning functions
Tests _clean_mongo_docs() and query methods that use it
"""
import unittest
import sys
import os
import importlib.util
from unittest.mock import Mock, MagicMock, patch
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime, date
from typing import List, Dict

# Import directly from files to avoid loading package __init__ which requires all dependencies
base_dir = os.path.join(os.path.dirname(__file__), '../../')
sys.path.insert(0, base_dir)

# Import converter
converter_path = os.path.join(base_dir, 'toonpy/core/converter.py')
spec = importlib.util.spec_from_file_location("converter", converter_path)
converter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(converter)
from_toon = converter.from_toon

# Mock toonpy package structure to prevent loading __init__
import types
toonpy_pkg = types.ModuleType('toonpy')
toonpy_adapters_pkg = types.ModuleType('toonpy.adapters')
toonpy_core_pkg = types.ModuleType('toonpy.core')
sys.modules['toonpy'] = toonpy_pkg
sys.modules['toonpy.adapters'] = toonpy_adapters_pkg
sys.modules['toonpy.core'] = toonpy_core_pkg

# Import base adapter first (mongo_adapter depends on it)
base_path = os.path.join(base_dir, 'toonpy/adapters/base.py')
spec = importlib.util.spec_from_file_location("toonpy.adapters.base", base_path)
base_adapter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(base_adapter)
sys.modules['toonpy.adapters.base'] = base_adapter

# Import mongo adapter
mongo_path = os.path.join(base_dir, 'toonpy/adapters/mongo_adapter.py')
spec = importlib.util.spec_from_file_location("toonpy.adapters.mongo_adapter", mongo_path)
mongo_adapter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mongo_adapter)
sys.modules['toonpy.adapters.mongo_adapter'] = mongo_adapter
MongoAdapter = mongo_adapter.MongoAdapter

# Also set up converter in sys.modules
sys.modules['toonpy.core.converter'] = converter


class TestCleanMongoDocs(unittest.TestCase):
    """Tests for _clean_mongo_docs() method"""
    
    def setUp(self):
        """Set up test adapter with mock collection"""
        self.mock_collection = Mock()
        self.adapter = MongoAdapter(collection=self.mock_collection)
    
    def test_basic_cleaning(self):
        """Test basic document cleaning"""
        docs = [
            {"_id": ObjectId(), "name": "Alice", "age": 30}
        ]
        cleaned = self.adapter._clean_mongo_docs(docs)
        self.assertEqual(len(cleaned), 1)
        self.assertEqual(cleaned[0]["name"], "Alice")
        self.assertEqual(cleaned[0]["age"], 30)
        # ObjectId should be converted to string
        self.assertIsInstance(cleaned[0]["_id"], str)
    
    def test_objectid_conversion(self):
        """Test ObjectId to string conversion"""
        oid = ObjectId()
        docs = [{"_id": oid, "name": "Test"}]
        cleaned = self.adapter._clean_mongo_docs(docs)
        self.assertIsInstance(cleaned[0]["_id"], str)
        self.assertEqual(cleaned[0]["_id"], str(oid))
    
    def test_multiple_objectids(self):
        """Test multiple ObjectIds in document"""
        oid1 = ObjectId()
        oid2 = ObjectId()
        docs = [
            {
                "_id": oid1,
                "user_id": oid2,
                "name": "Alice"
            }
        ]
        cleaned = self.adapter._clean_mongo_docs(docs)
        self.assertIsInstance(cleaned[0]["_id"], str)
        self.assertIsInstance(cleaned[0]["user_id"], str)
        self.assertEqual(cleaned[0]["_id"], str(oid1))
        self.assertEqual(cleaned[0]["user_id"], str(oid2))
    
    def test_datetime_conversion(self):
        """Test datetime to ISO string conversion"""
        dt = datetime(2024, 1, 15, 10, 30, 45)
        docs = [{"created": dt, "name": "Test"}]
        cleaned = self.adapter._clean_mongo_docs(docs)
        self.assertIsInstance(cleaned[0]["created"], str)
        self.assertEqual(cleaned[0]["created"], "2024-01-15T10:30:45")
    
    def test_date_conversion(self):
        """Test date to ISO string conversion"""
        d = date(2024, 1, 15)
        docs = [{"birthday": d, "name": "Test"}]
        cleaned = self.adapter._clean_mongo_docs(docs)
        self.assertIsInstance(cleaned[0]["birthday"], str)
        self.assertEqual(cleaned[0]["birthday"], "2024-01-15")
    
    def test_mixed_types(self):
        """Test cleaning with mixed MongoDB types"""
        oid = ObjectId()
        dt = datetime(2024, 1, 1)
        d = date(2024, 1, 1)
        docs = [
            {
                "_id": oid,
                "name": "Alice",
                "age": 30,
                "active": True,
                "score": 95.5,
                "created": dt,
                "birthday": d,
                "tags": ["admin", "user"],
                "metadata": None
            }
        ]
        cleaned = self.adapter._clean_mongo_docs(docs)
        self.assertIsInstance(cleaned[0]["_id"], str)
        self.assertEqual(cleaned[0]["name"], "Alice")
        self.assertEqual(cleaned[0]["age"], 30)
        self.assertEqual(cleaned[0]["active"], True)
        self.assertEqual(cleaned[0]["score"], 95.5)
        self.assertIsInstance(cleaned[0]["created"], str)
        self.assertIsInstance(cleaned[0]["birthday"], str)
        self.assertEqual(cleaned[0]["tags"], ["admin", "user"])
        self.assertEqual(cleaned[0]["metadata"], None)
    
    def test_empty_docs_list(self):
        """Test cleaning empty documents list"""
        docs = []
        cleaned = self.adapter._clean_mongo_docs(docs)
        self.assertEqual(cleaned, [])
    
    def test_multiple_documents(self):
        """Test cleaning multiple documents"""
        oid1 = ObjectId()
        oid2 = ObjectId()
        dt1 = datetime(2024, 1, 1)
        dt2 = datetime(2024, 1, 2)
        docs = [
            {"_id": oid1, "name": "Alice", "created": dt1},
            {"_id": oid2, "name": "Bob", "created": dt2}
        ]
        cleaned = self.adapter._clean_mongo_docs(docs)
        self.assertEqual(len(cleaned), 2)
        self.assertIsInstance(cleaned[0]["_id"], str)
        self.assertIsInstance(cleaned[1]["_id"], str)
        self.assertIsInstance(cleaned[0]["created"], str)
        self.assertIsInstance(cleaned[1]["created"], str)
    
    def test_preserves_regular_types(self):
        """Test that regular Python types are preserved"""
        docs = [
            {
                "_id": ObjectId(),
                "int_val": 42,
                "float_val": 3.14,
                "str_val": "test",
                "bool_val": True,
                "list_val": [1, 2, 3],
                "dict_val": {"key": "value"},
                "none_val": None
            }
        ]
        cleaned = self.adapter._clean_mongo_docs(docs)
        self.assertEqual(cleaned[0]["int_val"], 42)
        self.assertEqual(cleaned[0]["float_val"], 3.14)
        self.assertEqual(cleaned[0]["str_val"], "test")
        self.assertEqual(cleaned[0]["bool_val"], True)
        self.assertEqual(cleaned[0]["list_val"], [1, 2, 3])
        self.assertEqual(cleaned[0]["dict_val"], {"key": "value"})
        self.assertEqual(cleaned[0]["none_val"], None)
    
    def test_nested_objectids(self):
        """Test ObjectIds in nested structures (recursive cleaning)"""
        oid1 = ObjectId()
        oid2 = ObjectId()
        docs = [
            {
                "_id": oid1,
                "user": {
                    "user_id": oid2,
                    "name": "Alice"
                }
            }
        ]
        cleaned = self.adapter._clean_mongo_docs(docs)
        # Recursive cleaning should handle nested ObjectIds
        self.assertIsInstance(cleaned[0]["_id"], str)
        self.assertIsInstance(cleaned[0]["user"]["user_id"], str)
        self.assertEqual(cleaned[0]["user"]["user_id"], str(oid2))
        self.assertEqual(cleaned[0]["user"]["name"], "Alice")
    
    def test_nested_datetime(self):
        """Test datetime in nested structures (recursive cleaning)"""
        dt = datetime(2024, 1, 1)
        docs = [
            {
                "_id": ObjectId(),
                "user": {
                    "created": dt,
                    "name": "Alice"
                }
            }
        ]
        cleaned = self.adapter._clean_mongo_docs(docs)
        # Recursive cleaning should handle nested datetimes
        self.assertIsInstance(cleaned[0]["_id"], str)
        self.assertIsInstance(cleaned[0]["user"]["created"], str)
        self.assertEqual(cleaned[0]["user"]["created"], "2024-01-01T00:00:00")
        self.assertEqual(cleaned[0]["user"]["name"], "Alice")
    
    def test_all_mongodb_types(self):
        """Test all MongoDB-specific types"""
        oid = ObjectId()
        dt = datetime(2024, 1, 1, 12, 0, 0)
        d = date(2024, 1, 1)
        docs = [
            {
                "_id": oid,
                "created_at": dt,
                "birth_date": d,
                "regular_string": "test",
                "regular_int": 42
            }
        ]
        cleaned = self.adapter._clean_mongo_docs(docs)
        self.assertIsInstance(cleaned[0]["_id"], str)
        self.assertIsInstance(cleaned[0]["created_at"], str)
        self.assertIsInstance(cleaned[0]["birth_date"], str)
        self.assertEqual(cleaned[0]["regular_string"], "test")
        self.assertEqual(cleaned[0]["regular_int"], 42)


class TestFindMethod(unittest.TestCase):
    """Tests for find() method and its use of _clean_mongo_docs"""
    
    def setUp(self):
        """Set up test adapter with mock collection"""
        self.mock_collection = Mock()
        self.adapter = MongoAdapter(collection=self.mock_collection)
    
    def test_find_basic(self):
        """Test basic find operation"""
        oid1 = ObjectId()
        oid2 = ObjectId()
        mock_cursor = Mock()
        mock_cursor.__iter__ = Mock(return_value=iter([
            {"_id": oid1, "name": "Alice", "age": 30},
            {"_id": oid2, "name": "Bob", "age": 25}
        ]))
        self.mock_collection.find.return_value = mock_cursor
        
        result = self.adapter.find()
        
        self.assertIsInstance(result, str)
        # Decode and verify ObjectIds are converted
        decoded = from_toon(result)
        self.assertEqual(len(decoded), 2)
        self.assertIsInstance(decoded[0]["_id"], str)
        self.assertIsInstance(decoded[1]["_id"], str)
        self.assertEqual(decoded[0]["name"], "Alice")
        self.assertEqual(decoded[1]["name"], "Bob")
    
    def test_find_with_query(self):
        """Test find with query filter"""
        oid = ObjectId()
        mock_cursor = Mock()
        mock_cursor.__iter__ = Mock(return_value=iter([
            {"_id": oid, "name": "Alice", "role": "admin"}
        ]))
        self.mock_collection.find.return_value = mock_cursor
        
        result = self.adapter.find({"role": "admin"})
        
        decoded = from_toon(result)
        self.assertEqual(len(decoded), 1)
        self.assertIsInstance(decoded[0]["_id"], str)
        self.assertEqual(decoded[0]["name"], "Alice")
        self.assertEqual(decoded[0]["role"], "admin")
    
    def test_find_with_projection(self):
        """Test find with projection"""
        oid = ObjectId()
        mock_cursor = Mock()
        mock_cursor.__iter__ = Mock(return_value=iter([
            {"_id": oid, "name": "Alice"}
        ]))
        self.mock_collection.find.return_value = mock_cursor
        
        result = self.adapter.find(projection={"name": 1, "_id": 1})
        
        decoded = from_toon(result)
        self.assertEqual(len(decoded), 1)
        self.assertIsInstance(decoded[0]["_id"], str)
        self.assertEqual(decoded[0]["name"], "Alice")
    
    def test_find_with_datetime(self):
        """Test find with datetime in results"""
        oid = ObjectId()
        dt = datetime(2024, 1, 1)
        mock_cursor = Mock()
        mock_cursor.__iter__ = Mock(return_value=iter([
            {"_id": oid, "name": "Alice", "created": dt}
        ]))
        self.mock_collection.find.return_value = mock_cursor
        
        result = self.adapter.find()
        
        decoded = from_toon(result)
        self.assertIsInstance(decoded[0]["created"], str)
        self.assertEqual(decoded[0]["created"], "2024-01-01T00:00:00")
    
    def test_find_empty_result(self):
        """Test find with empty result"""
        mock_cursor = Mock()
        mock_cursor.__iter__ = Mock(return_value=iter([]))
        self.mock_collection.find.return_value = mock_cursor
        
        result = self.adapter.find()
        
        decoded = from_toon(result)
        self.assertEqual(decoded, [])


class TestFindOneMethod(unittest.TestCase):
    """Tests for find_one() method"""
    
    def setUp(self):
        """Set up test adapter with mock collection"""
        self.mock_collection = Mock()
        self.adapter = MongoAdapter(collection=self.mock_collection)
    
    def test_find_one_basic(self):
        """Test basic find_one operation"""
        oid = ObjectId()
        self.mock_collection.find_one.return_value = {
            "_id": oid,
            "name": "Alice",
            "age": 30
        }
        
        result = self.adapter.find_one()
        
        decoded = from_toon(result)
        self.assertEqual(len(decoded), 1)
        self.assertIsInstance(decoded[0]["_id"], str)
        self.assertEqual(decoded[0]["name"], "Alice")
    
    def test_find_one_not_found(self):
        """Test find_one when document not found"""
        self.mock_collection.find_one.return_value = None
        
        result = self.adapter.find_one()
        
        decoded = from_toon(result)
        self.assertEqual(decoded, [])
    
    def test_find_one_with_datetime(self):
        """Test find_one with datetime"""
        oid = ObjectId()
        dt = datetime(2024, 1, 1)
        self.mock_collection.find_one.return_value = {
            "_id": oid,
            "name": "Alice",
            "created": dt
        }
        
        result = self.adapter.find_one()
        
        decoded = from_toon(result)
        self.assertIsInstance(decoded[0]["created"], str)
        self.assertEqual(decoded[0]["created"], "2024-01-01T00:00:00")


class TestAggregateMethod(unittest.TestCase):
    """Tests for aggregate() method"""
    
    def setUp(self):
        """Set up test adapter with mock collection"""
        self.mock_collection = Mock()
        self.adapter = MongoAdapter(collection=self.mock_collection)
    
    def test_aggregate_basic(self):
        """Test basic aggregate operation"""
        oid1 = ObjectId()
        oid2 = ObjectId()
        mock_cursor = Mock()
        mock_cursor.__iter__ = Mock(return_value=iter([
            {"_id": oid1, "total": 100},
            {"_id": oid2, "total": 200}
        ]))
        self.mock_collection.aggregate.return_value = mock_cursor
        
        result = self.adapter.aggregate([{"$group": {"_id": None, "total": {"$sum": "$amount"}}}])
        
        decoded = from_toon(result)
        self.assertEqual(len(decoded), 2)
        self.assertIsInstance(decoded[0]["_id"], str)
        self.assertIsInstance(decoded[1]["_id"], str)
    
    def test_aggregate_with_datetime(self):
        """Test aggregate with datetime in results"""
        oid = ObjectId()
        dt = datetime(2024, 1, 1)
        mock_cursor = Mock()
        mock_cursor.__iter__ = Mock(return_value=iter([
            {"_id": oid, "date": dt, "count": 10}
        ]))
        self.mock_collection.aggregate.return_value = mock_cursor
        
        result = self.adapter.aggregate([{"$group": {"_id": "$date", "count": {"$sum": 1}}}])
        
        decoded = from_toon(result)
        self.assertIsInstance(decoded[0]["date"], str)
        self.assertEqual(decoded[0]["date"], "2024-01-01T00:00:00")


class TestDistinctMethod(unittest.TestCase):
    """Tests for distinct() method"""
    
    def setUp(self):
        """Set up test adapter with mock collection"""
        self.mock_collection = Mock()
        self.adapter = MongoAdapter(collection=self.mock_collection)
    
    def test_distinct_basic(self):
        """Test basic distinct operation"""
        self.mock_collection.distinct.return_value = ["admin", "user", "moderator"]
        
        result = self.adapter.distinct("role")
        
        decoded = from_toon(result)
        self.assertEqual(len(decoded), 3)
        self.assertEqual(decoded[0]["role"], "admin")
        self.assertEqual(decoded[1]["role"], "user")
        self.assertEqual(decoded[2]["role"], "moderator")
    
    def test_distinct_with_objectids(self):
        """Test distinct with ObjectId values"""
        oid1 = ObjectId()
        oid2 = ObjectId()
        self.mock_collection.distinct.return_value = [oid1, oid2]
        
        result = self.adapter.distinct("user_id")
        
        decoded = from_toon(result)
        self.assertEqual(len(decoded), 2)
        # ObjectIds should be converted to strings
        self.assertIsInstance(decoded[0]["user_id"], str)
        self.assertIsInstance(decoded[1]["user_id"], str)
    
    def test_distinct_with_filter(self):
        """Test distinct with filter"""
        self.mock_collection.distinct.return_value = ["active"]
        
        result = self.adapter.distinct("status", filter={"age": {"$gt": 30}})
        
        decoded = from_toon(result)
        self.assertEqual(len(decoded), 1)
        self.assertEqual(decoded[0]["status"], "active")


class TestQueryMethod(unittest.TestCase):
    """Tests for query() method (alias for find)"""
    
    def setUp(self):
        """Set up test adapter with mock collection"""
        self.mock_collection = Mock()
        self.adapter = MongoAdapter(collection=self.mock_collection)
    
    def test_query_with_dict(self):
        """Test query with dictionary"""
        oid = ObjectId()
        mock_cursor = Mock()
        mock_cursor.__iter__ = Mock(return_value=iter([
            {"_id": oid, "name": "Alice"}
        ]))
        self.mock_collection.find.return_value = mock_cursor
        
        result = self.adapter.query({"name": "Alice"})
        
        decoded = from_toon(result)
        self.assertEqual(len(decoded), 1)
        self.assertIsInstance(decoded[0]["_id"], str)
    
    def test_query_with_json_string(self):
        """Test query with JSON string"""
        oid = ObjectId()
        mock_cursor = Mock()
        mock_cursor.__iter__ = Mock(return_value=iter([
            {"_id": oid, "name": "Alice"}
        ]))
        self.mock_collection.find.return_value = mock_cursor
        
        result = self.adapter.query('{"name": "Alice"}')
        
        decoded = from_toon(result)
        self.assertEqual(len(decoded), 1)
        self.assertIsInstance(decoded[0]["_id"], str)
    
    def test_query_none(self):
        """Test query with None (should default to empty dict)"""
        mock_cursor = Mock()
        mock_cursor.__iter__ = Mock(return_value=iter([]))
        self.mock_collection.find.return_value = mock_cursor
        
        result = self.adapter.query(None)
        
        decoded = from_toon(result)
        self.assertEqual(decoded, [])


class TestMongoCleaningEdgeCases(unittest.TestCase):
    """Edge cases for MongoDB cleaning"""
    
    def setUp(self):
        """Set up test adapter with mock collection"""
        self.mock_collection = Mock()
        self.adapter = MongoAdapter(collection=self.mock_collection)
    
    def test_large_number_of_documents(self):
        """Test cleaning large number of documents"""
        docs = [
            {"_id": ObjectId(), "name": f"User{i}", "age": i}
            for i in range(1000)
        ]
        cleaned = self.adapter._clean_mongo_docs(docs)
        self.assertEqual(len(cleaned), 1000)
        self.assertIsInstance(cleaned[0]["_id"], str)
        self.assertIsInstance(cleaned[999]["_id"], str)
    
    def test_document_with_many_fields(self):
        """Test document with many fields"""
        oid = ObjectId()
        doc = {"_id": oid}
        for i in range(100):
            doc[f"field_{i}"] = i
        docs = [doc]
        cleaned = self.adapter._clean_mongo_docs(docs)
        self.assertEqual(len(cleaned[0]), 101)  # _id + 100 fields
        self.assertIsInstance(cleaned[0]["_id"], str)
    
    def test_all_none_values(self):
        """Test document with all None values"""
        oid = ObjectId()
        docs = [{"_id": oid, "a": None, "b": None, "c": None}]
        cleaned = self.adapter._clean_mongo_docs(docs)
        self.assertIsInstance(cleaned[0]["_id"], str)
        self.assertEqual(cleaned[0]["a"], None)
        self.assertEqual(cleaned[0]["b"], None)
        self.assertEqual(cleaned[0]["c"], None)
    
    def test_unicode_characters(self):
        """Test unicode characters in documents"""
        oid = ObjectId()
        docs = [
            {"_id": oid, "name": "José", "city": "São Paulo", "emoji": "🚀"}
        ]
        cleaned = self.adapter._clean_mongo_docs(docs)
        self.assertEqual(cleaned[0]["name"], "José")
        self.assertEqual(cleaned[0]["city"], "São Paulo")
        self.assertEqual(cleaned[0]["emoji"], "🚀")
    
    def test_special_characters_in_keys(self):
        """Test special characters in field names"""
        oid = ObjectId()
        docs = [
            {"_id": oid, "user-name": "Alice", "user_email": "alice@test.com"}
        ]
        cleaned = self.adapter._clean_mongo_docs(docs)
        self.assertEqual(cleaned[0]["user-name"], "Alice")
        self.assertEqual(cleaned[0]["user_email"], "alice@test.com")
    
    def test_deeply_nested_objectids(self):
        """Test deeply nested ObjectIds (3+ levels)"""
        oid1 = ObjectId()
        oid2 = ObjectId()
        oid3 = ObjectId()
        docs = [
            {
                "_id": oid1,
                "level1": {
                    "level2": {
                        "level3": {
                            "id": oid2,
                            "nested": {
                                "id": oid3
                            }
                        }
                    }
                }
            }
        ]
        cleaned = self.adapter._clean_mongo_docs(docs)
        self.assertIsInstance(cleaned[0]["_id"], str)
        self.assertIsInstance(cleaned[0]["level1"]["level2"]["level3"]["id"], str)
        self.assertIsInstance(cleaned[0]["level1"]["level2"]["level3"]["nested"]["id"], str)
        self.assertEqual(cleaned[0]["level1"]["level2"]["level3"]["id"], str(oid2))
        self.assertEqual(cleaned[0]["level1"]["level2"]["level3"]["nested"]["id"], str(oid3))
    
    def test_nested_datetime_in_list(self):
        """Test datetime in nested list structures"""
        dt1 = datetime(2024, 1, 1)
        dt2 = datetime(2024, 1, 2)
        docs = [
            {
                "_id": ObjectId(),
                "events": [
                    {"date": dt1, "name": "Event 1"},
                    {"date": dt2, "name": "Event 2"}
                ]
            }
        ]
        cleaned = self.adapter._clean_mongo_docs(docs)
        self.assertIsInstance(cleaned[0]["events"][0]["date"], str)
        self.assertIsInstance(cleaned[0]["events"][1]["date"], str)
        self.assertEqual(cleaned[0]["events"][0]["date"], "2024-01-01T00:00:00")
        self.assertEqual(cleaned[0]["events"][1]["date"], "2024-01-02T00:00:00")
    
    def test_mixed_nested_structures(self):
        """Test mixed nested structures with ObjectIds and datetimes"""
        oid1 = ObjectId()
        oid2 = ObjectId()
        dt = datetime(2024, 1, 1)
        docs = [
            {
                "_id": oid1,
                "users": [
                    {
                        "id": oid2,
                        "created": dt,
                        "profile": {
                            "updated": dt
                        }
                    }
                ]
            }
        ]
        cleaned = self.adapter._clean_mongo_docs(docs)
        self.assertIsInstance(cleaned[0]["users"][0]["id"], str)
        self.assertIsInstance(cleaned[0]["users"][0]["created"], str)
        self.assertIsInstance(cleaned[0]["users"][0]["profile"]["updated"], str)
        self.assertEqual(cleaned[0]["users"][0]["id"], str(oid2))
        self.assertEqual(cleaned[0]["users"][0]["created"], "2024-01-01T00:00:00")
        self.assertEqual(cleaned[0]["users"][0]["profile"]["updated"], "2024-01-01T00:00:00")
    
    def test_time_objects(self):
        """Test time objects in nested structures"""
        from datetime import time
        t = time(10, 30, 45)
        docs = [
            {
                "_id": ObjectId(),
                "schedule": {
                    "start_time": t
                }
            }
        ]
        cleaned = self.adapter._clean_mongo_docs(docs)
        self.assertIsInstance(cleaned[0]["schedule"]["start_time"], str)
        self.assertEqual(cleaned[0]["schedule"]["start_time"], "10:30:45")


if __name__ == '__main__':
    unittest.main()

