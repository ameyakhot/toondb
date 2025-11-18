"""
Intensive tests for PostgreSQL adapter cleaning functions
Tests _clean_postgres_data(), _clean_value(), and query methods that use them
"""
import unittest
import sys
import os
import importlib.util
from unittest.mock import Mock, MagicMock, patch
from decimal import Decimal
from datetime import datetime, date, time
from typing import List, Dict
import base64
import uuid

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

# Import exceptions first (needed by adapters)
exceptions_path = os.path.join(base_dir, 'toonpy/adapters/exceptions.py')
spec = importlib.util.spec_from_file_location("toonpy.adapters.exceptions", exceptions_path)
exceptions = importlib.util.module_from_spec(spec)
spec.loader.exec_module(exceptions)
sys.modules['toonpy.adapters.exceptions'] = exceptions

# Import base adapter first
base_path = os.path.join(base_dir, 'toonpy/adapters/base.py')
spec = importlib.util.spec_from_file_location("toonpy.adapters.base", base_path)
base_adapter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(base_adapter)
sys.modules['toonpy.adapters.base'] = base_adapter

# Mock psycopg2 before importing postgres adapter
mock_psycopg2 = types.ModuleType('psycopg2')
mock_psycopg2.OperationalError = Exception
mock_psycopg2.ProgrammingError = Exception
mock_psycopg2.IntegrityError = Exception
mock_psycopg2.connect = Mock()
sys.modules['psycopg2'] = mock_psycopg2
sys.modules['psycopg2.extras'] = types.ModuleType('psycopg2.extras')
sys.modules['psycopg2.extras'].RealDictCursor = Mock()
sys.modules['psycopg2.extensions'] = types.ModuleType('psycopg2.extensions')
sys.modules['psycopg2.extensions'].connection = Mock

# Import postgres adapter
postgres_path = os.path.join(base_dir, 'toonpy/adapters/postgres_adapter.py')
spec = importlib.util.spec_from_file_location("toonpy.adapters.postgres_adapter", postgres_path)
postgres_adapter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(postgres_adapter)
sys.modules['toonpy.adapters.postgres_adapter'] = postgres_adapter
PostgresAdapter = postgres_adapter.PostgresAdapter

# Also set up converter in sys.modules
sys.modules['toonpy.core.converter'] = converter


class TestCleanPostgresData(unittest.TestCase):
    """Tests for _clean_postgres_data() method"""
    
    def setUp(self):
        """Set up test adapter with mock connection"""
        self.mock_connection = Mock()
        self.mock_connection.closed = False
        self.adapter = PostgresAdapter(connection=self.mock_connection)
    
    def test_basic_cleaning(self):
        """Test basic data cleaning"""
        docs = [
            {"id": 1, "name": "Alice", "age": 30}
        ]
        cleaned = self.adapter._clean_postgres_data(docs)
        self.assertEqual(len(cleaned), 1)
        self.assertEqual(cleaned[0]["id"], 1)
        self.assertEqual(cleaned[0]["name"], "Alice")
        self.assertEqual(cleaned[0]["age"], 30)
    
    def test_decimal_conversion(self):
        """Test Decimal to float conversion"""
        docs = [
            {"price": Decimal("99.99"), "discount": Decimal("0.15")}
        ]
        cleaned = self.adapter._clean_postgres_data(docs)
        self.assertIsInstance(cleaned[0]["price"], float)
        self.assertIsInstance(cleaned[0]["discount"], float)
        self.assertEqual(cleaned[0]["price"], 99.99)
        self.assertEqual(cleaned[0]["discount"], 0.15)
    
    def test_datetime_conversion(self):
        """Test datetime to ISO string conversion"""
        dt = datetime(2024, 1, 15, 10, 30, 45)
        docs = [{"created": dt, "name": "Test"}]
        cleaned = self.adapter._clean_postgres_data(docs)
        self.assertIsInstance(cleaned[0]["created"], str)
        self.assertEqual(cleaned[0]["created"], "2024-01-15T10:30:45")
    
    def test_date_conversion(self):
        """Test date to ISO string conversion"""
        d = date(2024, 1, 15)
        docs = [{"birthday": d, "name": "Test"}]
        cleaned = self.adapter._clean_postgres_data(docs)
        self.assertIsInstance(cleaned[0]["birthday"], str)
        self.assertEqual(cleaned[0]["birthday"], "2024-01-15")
    
    def test_time_conversion(self):
        """Test time to ISO string conversion"""
        t = time(10, 30, 45)
        docs = [{"start_time": t, "name": "Test"}]
        cleaned = self.adapter._clean_postgres_data(docs)
        self.assertIsInstance(cleaned[0]["start_time"], str)
        self.assertEqual(cleaned[0]["start_time"], "10:30:45")
    
    def test_uuid_conversion(self):
        """Test UUID to string conversion (PostgreSQL specific)"""
        test_uuid = uuid.uuid4()
        docs = [{"id": test_uuid, "name": "Test"}]
        cleaned = self.adapter._clean_postgres_data(docs)
        self.assertIsInstance(cleaned[0]["id"], str)
        self.assertEqual(cleaned[0]["id"], str(test_uuid))
    
    def test_bytes_conversion(self):
        """Test bytes (bytea) to base64 string conversion"""
        blob_data = b"binary data here"
        docs = [{"image": blob_data, "name": "Test"}]
        cleaned = self.adapter._clean_postgres_data(docs)
        self.assertIsInstance(cleaned[0]["image"], str)
        # Should be base64 encoded
        decoded = base64.b64decode(cleaned[0]["image"])
        self.assertEqual(decoded, blob_data)
    
    def test_list_conversion(self):
        """Test list/tuple conversion (PostgreSQL arrays)"""
        docs = [
            {"tags": ["admin", "user"], "scores": (95, 87, 92)}
        ]
        cleaned = self.adapter._clean_postgres_data(docs)
        self.assertIsInstance(cleaned[0]["tags"], list)
        self.assertIsInstance(cleaned[0]["scores"], list)  # Tuple converted to list
        self.assertEqual(cleaned[0]["tags"], ["admin", "user"])
        self.assertEqual(cleaned[0]["scores"], [95, 87, 92])
    
    def test_dict_conversion(self):
        """Test dict conversion (JSON/JSONB types)"""
        docs = [
            {"metadata": {"key": "value", "nested": {"inner": 42}}}
        ]
        cleaned = self.adapter._clean_postgres_data(docs)
        self.assertIsInstance(cleaned[0]["metadata"], dict)
        self.assertEqual(cleaned[0]["metadata"]["key"], "value")
        self.assertEqual(cleaned[0]["metadata"]["nested"]["inner"], 42)
    
    def test_nested_decimal(self):
        """Test Decimal in nested structures"""
        docs = [
            {"data": {"price": Decimal("99.99"), "tax": Decimal("9.99")}}
        ]
        cleaned = self.adapter._clean_postgres_data(docs)
        self.assertIsInstance(cleaned[0]["data"]["price"], float)
        self.assertIsInstance(cleaned[0]["data"]["tax"], float)
    
    def test_nested_datetime(self):
        """Test datetime in nested structures"""
        dt = datetime(2024, 1, 1)
        docs = [
            {"user": {"name": "Alice", "created": dt}}
        ]
        cleaned = self.adapter._clean_postgres_data(docs)
        self.assertIsInstance(cleaned[0]["user"]["created"], str)
        self.assertEqual(cleaned[0]["user"]["created"], "2024-01-01T00:00:00")
    
    def test_nested_uuid(self):
        """Test UUID in nested structures"""
        test_uuid = uuid.uuid4()
        docs = [
            {"data": {"id": test_uuid, "name": "Test"}}
        ]
        cleaned = self.adapter._clean_postgres_data(docs)
        self.assertIsInstance(cleaned[0]["data"]["id"], str)
        self.assertEqual(cleaned[0]["data"]["id"], str(test_uuid))
    
    def test_nested_bytes(self):
        """Test bytes in nested structures"""
        blob_data = b"nested binary"
        docs = [
            {"data": {"image": blob_data}}
        ]
        cleaned = self.adapter._clean_postgres_data(docs)
        self.assertIsInstance(cleaned[0]["data"]["image"], str)
        decoded = base64.b64decode(cleaned[0]["data"]["image"])
        self.assertEqual(decoded, blob_data)
    
    def test_mixed_types(self):
        """Test cleaning with mixed PostgreSQL types"""
        dt = datetime(2024, 1, 1)
        d = date(2024, 1, 1)
        t = time(10, 30)
        test_uuid = uuid.uuid4()
        blob = b"binary"
        docs = [
            {
                "id": test_uuid,
                "name": "Alice",
                "price": Decimal("99.99"),
                "created": dt,
                "birthday": d,
                "start_time": t,
                "image": blob,
                "tags": ["admin", "user"],
                "metadata": {"key": "value"}
            }
        ]
        cleaned = self.adapter._clean_postgres_data(docs)
        self.assertIsInstance(cleaned[0]["id"], str)
        self.assertEqual(cleaned[0]["name"], "Alice")
        self.assertIsInstance(cleaned[0]["price"], float)
        self.assertIsInstance(cleaned[0]["created"], str)
        self.assertIsInstance(cleaned[0]["birthday"], str)
        self.assertIsInstance(cleaned[0]["start_time"], str)
        self.assertIsInstance(cleaned[0]["image"], str)
        self.assertIsInstance(cleaned[0]["tags"], list)
        self.assertIsInstance(cleaned[0]["metadata"], dict)
    
    def test_empty_docs_list(self):
        """Test cleaning empty documents list"""
        docs = []
        cleaned = self.adapter._clean_postgres_data(docs)
        self.assertEqual(cleaned, [])
    
    def test_multiple_documents(self):
        """Test cleaning multiple documents"""
        test_uuid1 = uuid.uuid4()
        test_uuid2 = uuid.uuid4()
        docs = [
            {"id": test_uuid1, "name": "Alice", "price": Decimal("99.99")},
            {"id": test_uuid2, "name": "Bob", "price": Decimal("149.99")}
        ]
        cleaned = self.adapter._clean_postgres_data(docs)
        self.assertEqual(len(cleaned), 2)
        self.assertIsInstance(cleaned[0]["id"], str)
        self.assertIsInstance(cleaned[1]["id"], str)
        self.assertIsInstance(cleaned[0]["price"], float)
        self.assertIsInstance(cleaned[1]["price"], float)
    
    def test_none_values(self):
        """Test handling of None values"""
        docs = [
            {"id": 1, "name": None, "age": 30, "price": None}
        ]
        cleaned = self.adapter._clean_postgres_data(docs)
        self.assertEqual(cleaned[0]["id"], 1)
        self.assertEqual(cleaned[0]["name"], None)
        self.assertEqual(cleaned[0]["age"], 30)
        self.assertEqual(cleaned[0]["price"], None)
    
    def test_preserves_regular_types(self):
        """Test that regular Python types are preserved"""
        docs = [
            {
                "int_val": 42,
                "float_val": 3.14,
                "str_val": "test",
                "bool_val": True,
                "none_val": None
            }
        ]
        cleaned = self.adapter._clean_postgres_data(docs)
        self.assertEqual(cleaned[0]["int_val"], 42)
        self.assertEqual(cleaned[0]["float_val"], 3.14)
        self.assertEqual(cleaned[0]["str_val"], "test")
        self.assertEqual(cleaned[0]["bool_val"], True)
        self.assertEqual(cleaned[0]["none_val"], None)
    
    def test_custom_type_fallback(self):
        """Test custom types fallback to string"""
        class CustomType:
            def __str__(self):
                return "custom_value"
        
        custom_val = CustomType()
        docs = [{"status": custom_val}]
        cleaned = self.adapter._clean_postgres_data(docs)
        self.assertIsInstance(cleaned[0]["status"], str)
        self.assertEqual(cleaned[0]["status"], "custom_value")
    
    def test_list_with_decimal(self):
        """Test list containing Decimal values"""
        docs = [
            {"prices": [Decimal("10.50"), Decimal("20.75"), Decimal("30.00")]}
        ]
        cleaned = self.adapter._clean_postgres_data(docs)
        self.assertIsInstance(cleaned[0]["prices"], list)
        self.assertIsInstance(cleaned[0]["prices"][0], float)
        self.assertEqual(cleaned[0]["prices"], [10.50, 20.75, 30.00])
    
    def test_list_with_datetime(self):
        """Test list containing datetime values"""
        dt1 = datetime(2024, 1, 1)
        dt2 = datetime(2024, 1, 2)
        docs = [
            {"dates": [dt1, dt2]}
        ]
        cleaned = self.adapter._clean_postgres_data(docs)
        self.assertIsInstance(cleaned[0]["dates"], list)
        self.assertIsInstance(cleaned[0]["dates"][0], str)
        self.assertEqual(cleaned[0]["dates"], ["2024-01-01T00:00:00", "2024-01-02T00:00:00"])
    
    def test_list_with_uuid(self):
        """Test list containing UUID values"""
        uuid1 = uuid.uuid4()
        uuid2 = uuid.uuid4()
        docs = [
            {"ids": [uuid1, uuid2]}
        ]
        cleaned = self.adapter._clean_postgres_data(docs)
        self.assertIsInstance(cleaned[0]["ids"], list)
        self.assertIsInstance(cleaned[0]["ids"][0], str)
        self.assertEqual(cleaned[0]["ids"], [str(uuid1), str(uuid2)])


class TestQueryMethod(unittest.TestCase):
    """Tests for query() method and its use of _clean_postgres_data"""
    
    def setUp(self):
        """Set up test adapter with mock connection"""
        self.mock_connection = Mock()
        self.mock_connection.closed = False
        self.adapter = PostgresAdapter(connection=self.mock_connection)
    
    def test_query_with_decimal(self):
        """Test query with Decimal in results"""
        mock_cursor = Mock()
        mock_cursor.description = [("id",), ("price",)]  # Has description = SELECT query
        mock_cursor.fetchall.return_value = [
            {"id": 1, "price": Decimal("99.99")}
        ]
        self.mock_connection.cursor.return_value = mock_cursor
        
        result = self.adapter.query("SELECT id, price FROM products")
        
        decoded = from_toon(result)
        self.assertEqual(len(decoded), 1)
        self.assertIsInstance(decoded[0]["price"], float)
        self.assertEqual(decoded[0]["price"], 99.99)
    
    def test_query_with_datetime(self):
        """Test query with datetime in results"""
        dt = datetime(2024, 1, 1)
        mock_cursor = Mock()
        mock_cursor.description = [("id",), ("created",)]
        mock_cursor.fetchall.return_value = [
            {"id": 1, "created": dt}
        ]
        self.mock_connection.cursor.return_value = mock_cursor
        
        result = self.adapter.query("SELECT id, created FROM users")
        
        decoded = from_toon(result)
        self.assertIsInstance(decoded[0]["created"], str)
        self.assertEqual(decoded[0]["created"], "2024-01-01T00:00:00")
    
    def test_query_with_uuid(self):
        """Test query with UUID in results"""
        test_uuid = uuid.uuid4()
        mock_cursor = Mock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [
            {"id": test_uuid, "name": "Alice"}
        ]
        self.mock_connection.cursor.return_value = mock_cursor
        
        result = self.adapter.query("SELECT id, name FROM users")
        
        decoded = from_toon(result)
        self.assertIsInstance(decoded[0]["id"], str)
        self.assertEqual(decoded[0]["id"], str(test_uuid))
    
    def test_query_with_bytes(self):
        """Test query with bytes (bytea) in results"""
        blob_data = b"binary data"
        mock_cursor = Mock()
        mock_cursor.description = [("id",), ("image",)]
        mock_cursor.fetchall.return_value = [
            {"id": 1, "image": blob_data}
        ]
        self.mock_connection.cursor.return_value = mock_cursor
        
        result = self.adapter.query("SELECT id, image FROM products")
        
        decoded = from_toon(result)
        self.assertIsInstance(decoded[0]["image"], str)
        decoded_bytes = base64.b64decode(decoded[0]["image"])
        self.assertEqual(decoded_bytes, blob_data)
    
    def test_query_empty_result(self):
        """Test query with empty result"""
        mock_cursor = Mock()
        mock_cursor.description = [("id",)]
        mock_cursor.fetchall.return_value = []
        self.mock_connection.cursor.return_value = mock_cursor
        
        result = self.adapter.query("SELECT id FROM users WHERE id = 999")
        
        decoded = from_toon(result)
        self.assertEqual(decoded, [])


class TestExecuteMethod(unittest.TestCase):
    """Tests for execute() method (alias for query)"""
    
    def setUp(self):
        """Set up test adapter with mock connection"""
        self.mock_connection = Mock()
        self.mock_connection.closed = False
        self.adapter = PostgresAdapter(connection=self.mock_connection)
    
    def test_execute_alias(self):
        """Test that execute is alias for query"""
        mock_cursor = Mock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [
            {"id": 1, "name": "Alice"}
        ]
        self.mock_connection.cursor.return_value = mock_cursor
        
        result = self.adapter.execute("SELECT id, name FROM users")
        
        decoded = from_toon(result)
        self.assertEqual(len(decoded), 1)
        self.assertEqual(decoded[0]["name"], "Alice")


class TestPostgresCleaningEdgeCases(unittest.TestCase):
    """Edge cases for PostgreSQL cleaning"""
    
    def setUp(self):
        """Set up test adapter with mock connection"""
        self.mock_connection = Mock()
        self.mock_connection.closed = False
        self.adapter = PostgresAdapter(connection=self.mock_connection)
    
    def test_large_number_of_documents(self):
        """Test cleaning large number of documents"""
        docs = [
            {"id": uuid.uuid4(), "price": Decimal(f"{i}.99")}
            for i in range(1000)
        ]
        cleaned = self.adapter._clean_postgres_data(docs)
        self.assertEqual(len(cleaned), 1000)
        self.assertIsInstance(cleaned[0]["id"], str)
        self.assertIsInstance(cleaned[0]["price"], float)
        self.assertIsInstance(cleaned[999]["price"], float)
    
    def test_document_with_many_fields(self):
        """Test document with many fields"""
        doc = {}
        for i in range(100):
            doc[f"field_{i}"] = Decimal(f"{i}.50")
        docs = [doc]
        cleaned = self.adapter._clean_postgres_data(docs)
        self.assertEqual(len(cleaned[0]), 100)
        self.assertIsInstance(cleaned[0]["field_0"], float)
        self.assertIsInstance(cleaned[0]["field_99"], float)
    
    def test_unicode_characters(self):
        """Test unicode characters in data"""
        docs = [
            {"name": "José", "city": "São Paulo", "emoji": "🚀"}
        ]
        cleaned = self.adapter._clean_postgres_data(docs)
        self.assertEqual(cleaned[0]["name"], "José")
        self.assertEqual(cleaned[0]["city"], "São Paulo")
        self.assertEqual(cleaned[0]["emoji"], "🚀")
    
    def test_special_characters_in_keys(self):
        """Test special characters in field names"""
        docs = [
            {"user-name": "Alice", "user_email": "alice@test.com"}
        ]
        cleaned = self.adapter._clean_postgres_data(docs)
        self.assertEqual(cleaned[0]["user-name"], "Alice")
        self.assertEqual(cleaned[0]["user_email"], "alice@test.com")
    
    def test_deeply_nested_structures(self):
        """Test deeply nested structures"""
        test_uuid = uuid.uuid4()
        docs = [
            {
                "level1": {
                    "level2": {
                        "level3": {
                            "id": test_uuid,
                            "price": Decimal("99.99"),
                            "created": datetime(2024, 1, 1)
                        }
                    }
                }
            }
        ]
        cleaned = self.adapter._clean_postgres_data(docs)
        self.assertIsInstance(cleaned[0]["level1"]["level2"]["level3"]["id"], str)
        self.assertIsInstance(cleaned[0]["level1"]["level2"]["level3"]["price"], float)
        self.assertIsInstance(cleaned[0]["level1"]["level2"]["level3"]["created"], str)


if __name__ == '__main__':
    unittest.main()

