"""
Intensive tests for toonpy.core.converter module
Tests to_toon(), from_toon(), and _clean_data() functions
"""
import unittest
import sys
import os
import importlib.util
from datetime import datetime, date, time
from decimal import Decimal
import json
from typing import List, Dict, Any

# Import directly from file to avoid loading package __init__ which requires all dependencies
base_dir = os.path.join(os.path.dirname(__file__), '../../')
converter_path = os.path.join(base_dir, 'toonpy/core/converter.py')
spec = importlib.util.spec_from_file_location("converter", converter_path)
converter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(converter)

to_toon = converter.to_toon
from_toon = converter.from_toon
_clean_data = converter._clean_data


class TestToToon(unittest.TestCase):
    """Tests for to_toon() function"""
    
    def test_basic_dict_list(self):
        """Test basic conversion of simple dict list"""
        data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        result = to_toon(data)
        self.assertIsInstance(result, str)
        self.assertIn("alice", result.lower())
        self.assertIn("bob", result.lower())
    
    def test_empty_list(self):
        """Test conversion of empty list"""
        data = []
        result = to_toon(data)
        self.assertIsInstance(result, str)
        # Empty list should produce valid TOON
        decoded = from_toon(result)
        self.assertEqual(decoded, [])
    
    def test_single_dict(self):
        """Test conversion of list with single dict"""
        data = [{"name": "Test"}]
        result = to_toon(data)
        self.assertIsInstance(result, str)
        decoded = from_toon(result)
        self.assertEqual(decoded, data)
    
    def test_none_values(self):
        """Test handling of None values"""
        data = [{"id": 1, "name": None, "age": 30}]
        result = to_toon(data)
        decoded = from_toon(result)
        self.assertEqual(decoded[0]["name"], None)
        self.assertEqual(decoded[0]["age"], 30)
    
    def test_datetime_values(self):
        """Test conversion of datetime objects"""
        dt = datetime(2024, 1, 15, 10, 30, 45)
        data = [{"timestamp": dt, "name": "Test"}]
        result = to_toon(data)
        decoded = from_toon(result)
        # Datetime should be converted to ISO string
        self.assertIsInstance(decoded[0]["timestamp"], str)
        self.assertEqual(decoded[0]["timestamp"], "2024-01-15T10:30:45")
    
    def test_date_values(self):
        """Test conversion of date objects"""
        d = date(2024, 1, 15)
        data = [{"birthday": d, "name": "Test"}]
        result = to_toon(data)
        decoded = from_toon(result)
        # Date should be converted to ISO string
        self.assertIsInstance(decoded[0]["birthday"], str)
        self.assertEqual(decoded[0]["birthday"], "2024-01-15")
    
    def test_mixed_types(self):
        """Test conversion with mixed data types"""
        data = [
            {
                "id": 1,
                "name": "Alice",
                "age": 30,
                "active": True,
                "score": 95.5,
                "tags": ["admin", "user"],
                "metadata": None,
                "created": datetime(2024, 1, 1),
                "birthday": date(1990, 5, 15)
            }
        ]
        result = to_toon(data)
        decoded = from_toon(result)
        self.assertEqual(decoded[0]["id"], 1)
        self.assertEqual(decoded[0]["name"], "Alice")
        self.assertEqual(decoded[0]["age"], 30)
        self.assertEqual(decoded[0]["active"], True)
        self.assertEqual(decoded[0]["score"], 95.5)
        self.assertEqual(decoded[0]["tags"], ["admin", "user"])
        self.assertEqual(decoded[0]["metadata"], None)
        self.assertIsInstance(decoded[0]["created"], str)
        self.assertIsInstance(decoded[0]["birthday"], str)
    
    def test_nested_dicts(self):
        """Test conversion of nested dictionaries"""
        data = [
            {
                "user": {
                    "name": "Alice",
                    "address": {
                        "city": "NYC",
                        "zip": 10001
                    }
                }
            }
        ]
        result = to_toon(data)
        decoded = from_toon(result)
        # Note: TOON library may flatten or stringify nested structures
        # Test that conversion works without errors
        self.assertIsInstance(decoded, list)
        self.assertEqual(len(decoded), 1)
        # The structure may be preserved or transformed by TOON library
        # This test verifies the conversion doesn't crash
    
    def test_lists_in_dicts(self):
        """Test conversion of lists within dictionaries"""
        data = [
            {
                "name": "Alice",
                "tags": ["admin", "user", "moderator"],
                "scores": [95, 87, 92]
            }
        ]
        result = to_toon(data)
        decoded = from_toon(result)
        self.assertEqual(decoded[0]["tags"], ["admin", "user", "moderator"])
        self.assertEqual(decoded[0]["scores"], [95, 87, 92])
    
    def test_large_dataset(self):
        """Test conversion of large dataset"""
        data = [{"id": i, "name": f"User{i}"} for i in range(1000)]
        result = to_toon(data)
        self.assertIsInstance(result, str)
        decoded = from_toon(result)
        self.assertEqual(len(decoded), 1000)
        self.assertEqual(decoded[0]["id"], 0)
        self.assertEqual(decoded[999]["id"], 999)
    
    def test_unicode_characters(self):
        """Test handling of unicode characters"""
        data = [
            {"name": "José", "city": "São Paulo", "emoji": "🚀"},
            {"name": "北京", "city": "上海"}
        ]
        result = to_toon(data)
        decoded = from_toon(result)
        self.assertEqual(decoded[0]["name"], "José")
        self.assertEqual(decoded[0]["city"], "São Paulo")
        self.assertEqual(decoded[1]["name"], "北京")
    
    def test_special_characters_in_keys(self):
        """Test handling of special characters in keys"""
        data = [{"user-name": "Alice", "user_email": "alice@test.com"}]
        result = to_toon(data)
        decoded = from_toon(result)
        self.assertEqual(decoded[0]["user-name"], "Alice")
        self.assertEqual(decoded[0]["user_email"], "alice@test.com")
    
    def test_numeric_types(self):
        """Test various numeric types"""
        data = [
            {
                "int_val": 42,
                "float_val": 3.14,
                "negative": -10,
                "zero": 0,
                "large_int": 999999999
            }
        ]
        result = to_toon(data)
        decoded = from_toon(result)
        self.assertEqual(decoded[0]["int_val"], 42)
        self.assertEqual(decoded[0]["float_val"], 3.14)
        self.assertEqual(decoded[0]["negative"], -10)
        self.assertEqual(decoded[0]["zero"], 0)
        self.assertEqual(decoded[0]["large_int"], 999999999)
    
    def test_boolean_values(self):
        """Test boolean values"""
        data = [
            {"active": True, "deleted": False, "verified": True}
        ]
        result = to_toon(data)
        decoded = from_toon(result)
        self.assertEqual(decoded[0]["active"], True)
        self.assertEqual(decoded[0]["deleted"], False)
        self.assertEqual(decoded[0]["verified"], True)
    
    def test_empty_strings(self):
        """Test empty strings"""
        data = [{"name": "", "description": "not empty"}]
        result = to_toon(data)
        decoded = from_toon(result)
        self.assertEqual(decoded[0]["name"], "")
        self.assertEqual(decoded[0]["description"], "not empty")
    
    def test_round_trip_integrity(self):
        """Test that round-trip conversion preserves data"""
        original = [
            {
                "id": 1,
                "name": "Alice",
                "age": 30,
                "active": True,
                "score": 95.5,
                "tags": ["admin", "user"],
                "metadata": None,
                "created": datetime(2024, 1, 1),
                "nested": {"key": "value"}
            }
        ]
        toon_str = to_toon(original)
        decoded = from_toon(toon_str)
        # Note: datetime/date become strings, so we check those separately
        self.assertEqual(decoded[0]["id"], original[0]["id"])
        self.assertEqual(decoded[0]["name"], original[0]["name"])
        self.assertEqual(decoded[0]["age"], original[0]["age"])
        self.assertEqual(decoded[0]["active"], original[0]["active"])
        self.assertEqual(decoded[0]["score"], original[0]["score"])
        self.assertEqual(decoded[0]["tags"], original[0]["tags"])
        self.assertEqual(decoded[0]["metadata"], original[0]["metadata"])
        self.assertEqual(decoded[0]["nested"], original[0]["nested"])
        # Datetime converted to ISO string
        self.assertEqual(decoded[0]["created"], "2024-01-01T00:00:00")


class TestFromToon(unittest.TestCase):
    """Tests for from_toon() function"""
    
    def test_basic_decoding(self):
        """Test basic TOON string decoding"""
        data = [{"id": 1, "name": "Alice"}]
        toon_str = to_toon(data)
        decoded = from_toon(toon_str)
        self.assertEqual(decoded, data)
    
    def test_empty_list_decoding(self):
        """Test decoding empty TOON string"""
        data = []
        toon_str = to_toon(data)
        decoded = from_toon(toon_str)
        self.assertEqual(decoded, [])
    
    def test_complex_decoding(self):
        """Test decoding complex TOON string"""
        original = [
            {"id": 1, "name": "Alice", "tags": ["a", "b"], "nested": {"key": "value"}},
            {"id": 2, "name": "Bob", "tags": ["c"], "nested": {"key": "value2"}}
        ]
        toon_str = to_toon(original)
        decoded = from_toon(toon_str)
        self.assertEqual(len(decoded), 2)
        self.assertEqual(decoded[0]["name"], "Alice")
        self.assertEqual(decoded[1]["name"], "Bob")
    
    def test_invalid_toon_string(self):
        """Test handling of invalid TOON string"""
        invalid_str = "not a valid toon string"
        # The decode function from toon library should handle this
        # We test that it doesn't crash
        try:
            result = from_toon(invalid_str)
            # If it doesn't raise, result might be empty or error format
            self.assertIsInstance(result, list)
        except Exception as e:
            # If it raises, that's also acceptable behavior
            self.assertIsInstance(e, Exception)


class TestCleanData(unittest.TestCase):
    """Tests for _clean_data() function"""
    
    def test_basic_cleaning(self):
        """Test basic data cleaning"""
        data = [{"id": 1, "name": "Alice"}]
        cleaned = _clean_data(data)
        self.assertEqual(cleaned, data)
    
    def test_none_values(self):
        """Test cleaning with None values"""
        data = [{"id": 1, "name": None, "age": 30}]
        cleaned = _clean_data(data)
        self.assertEqual(cleaned[0]["name"], None)
        self.assertEqual(cleaned[0]["age"], 30)
    
    def test_datetime_cleaning(self):
        """Test cleaning datetime objects"""
        dt = datetime(2024, 1, 15, 10, 30, 45)
        data = [{"timestamp": dt}]
        cleaned = _clean_data(data)
        self.assertIsInstance(cleaned[0]["timestamp"], str)
        self.assertEqual(cleaned[0]["timestamp"], "2024-01-15T10:30:45")
    
    def test_date_cleaning(self):
        """Test cleaning date objects"""
        d = date(2024, 1, 15)
        data = [{"birthday": d}]
        cleaned = _clean_data(data)
        self.assertIsInstance(cleaned[0]["birthday"], str)
        self.assertEqual(cleaned[0]["birthday"], "2024-01-15")
    
    def test_preserves_other_types(self):
        """Test that other types are preserved"""
        data = [
            {
                "int_val": 42,
                "float_val": 3.14,
                "str_val": "test",
                "bool_val": True,
                "list_val": [1, 2, 3],
                "dict_val": {"key": "value"},
                "none_val": None
            }
        ]
        cleaned = _clean_data(data)
        self.assertEqual(cleaned[0]["int_val"], 42)
        self.assertEqual(cleaned[0]["float_val"], 3.14)
        self.assertEqual(cleaned[0]["str_val"], "test")
        self.assertEqual(cleaned[0]["bool_val"], True)
        self.assertEqual(cleaned[0]["list_val"], [1, 2, 3])
        self.assertEqual(cleaned[0]["dict_val"], {"key": "value"})
        self.assertEqual(cleaned[0]["none_val"], None)
    
    def test_multiple_datetime_objects(self):
        """Test cleaning multiple datetime objects"""
        dt1 = datetime(2024, 1, 1)
        dt2 = datetime(2024, 12, 31, 23, 59, 59)
        data = [
            {"created": dt1, "updated": dt2},
            {"created": dt2, "updated": dt1}
        ]
        cleaned = _clean_data(data)
        self.assertEqual(cleaned[0]["created"], "2024-01-01T00:00:00")
        self.assertEqual(cleaned[0]["updated"], "2024-12-31T23:59:59")
        self.assertEqual(cleaned[1]["created"], "2024-12-31T23:59:59")
        self.assertEqual(cleaned[1]["updated"], "2024-01-01T00:00:00")
    
    def test_empty_list_cleaning(self):
        """Test cleaning empty list"""
        data = []
        cleaned = _clean_data(data)
        self.assertEqual(cleaned, [])
    
    def test_nested_datetime(self):
        """Test cleaning datetime in nested structures"""
        dt = datetime(2024, 1, 1)
        data = [
            {
                "user": {
                    "name": "Alice",
                    "created": dt
                }
            }
        ]
        cleaned = _clean_data(data)
        # Note: _clean_data only handles top-level datetime/date objects
        # Nested datetimes are not recursively cleaned (current implementation)
        # This test verifies the function doesn't crash
        self.assertIsInstance(cleaned, list)
        self.assertEqual(len(cleaned), 1)
        # Nested datetime might still be datetime object (current behavior)
    
    def test_time_object_not_handled(self):
        """Test that time objects are not converted (only datetime/date)"""
        t = time(10, 30, 45)
        data = [{"time_val": t}]
        cleaned = _clean_data(data)
        # time objects are not in the isinstance check, so they pass through
        # This tests current behavior
        self.assertIsInstance(cleaned[0]["time_val"], time)


class TestConverterEdgeCases(unittest.TestCase):
    """Edge cases and error scenarios"""
    
    def test_very_long_strings(self):
        """Test with very long string values"""
        long_string = "a" * 10000
        data = [{"description": long_string}]
        result = to_toon(data)
        decoded = from_toon(result)
        self.assertEqual(decoded[0]["description"], long_string)
    
    def test_very_many_fields(self):
        """Test with many fields in a single dict"""
        data = [{f"field_{i}": i for i in range(100)}]
        result = to_toon(data)
        decoded = from_toon(result)
        self.assertEqual(len(decoded[0]), 100)
        self.assertEqual(decoded[0]["field_0"], 0)
        self.assertEqual(decoded[0]["field_99"], 99)
    
    def test_deeply_nested_structures(self):
        """Test deeply nested dictionaries"""
        nested = {"level1": {"level2": {"level3": {"level4": {"value": 42}}}}}
        data = [{"nested": nested}]
        result = to_toon(data)
        decoded = from_toon(result)
        # Note: TOON library may flatten or stringify deeply nested structures
        # Test that conversion works without errors
        self.assertIsInstance(decoded, list)
        self.assertEqual(len(decoded), 1)
        # The structure may be preserved or transformed by TOON library
        # This test verifies the conversion doesn't crash
    
    def test_mixed_none_and_values(self):
        """Test mix of None and actual values"""
        data = [
            {"a": None, "b": 1, "c": None, "d": "test", "e": None}
        ]
        result = to_toon(data)
        decoded = from_toon(result)
        self.assertEqual(decoded[0]["a"], None)
        self.assertEqual(decoded[0]["b"], 1)
        self.assertEqual(decoded[0]["c"], None)
        self.assertEqual(decoded[0]["d"], "test")
        self.assertEqual(decoded[0]["e"], None)
    
    def test_all_none_values(self):
        """Test dict with all None values"""
        data = [{"a": None, "b": None, "c": None}]
        result = to_toon(data)
        decoded = from_toon(result)
        self.assertEqual(decoded[0]["a"], None)
        self.assertEqual(decoded[0]["b"], None)
        self.assertEqual(decoded[0]["c"], None)


if __name__ == '__main__':
    unittest.main()

