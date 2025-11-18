"""
Tests for statistics tracking classes
"""
import unittest
import sys
import os
import importlib.util
import time

# Import directly from files to avoid loading package __init__ which requires all dependencies
base_dir = os.path.join(os.path.dirname(__file__), '../../')
sys.path.insert(0, base_dir)

# Import stats
stats_path = os.path.join(base_dir, 'toonpy/core/stats.py')
spec = importlib.util.spec_from_file_location("stats", stats_path)
stats = importlib.util.module_from_spec(spec)
spec.loader.exec_module(stats)

QueryStats = stats.QueryStats
SessionStats = stats.SessionStats


class TestQueryStats(unittest.TestCase):
    """Tests for QueryStats dataclass"""
    
    def test_basic_creation(self):
        """Test basic QueryStats creation"""
        qs = QueryStats(
            json_chars=100,
            json_tokens=25,
            toon_chars=40,
            toon_tokens=10,
            tokenizer_name="gpt-4",
            query_type="query"
        )
        self.assertEqual(qs.json_chars, 100)
        self.assertEqual(qs.json_tokens, 25)
        self.assertEqual(qs.toon_chars, 40)
        self.assertEqual(qs.toon_tokens, 10)
        self.assertEqual(qs.tokenizer_name, "gpt-4")
        self.assertEqual(qs.query_type, "query")
    
    def test_savings_chars_percent(self):
        """Test savings_chars_percent calculation"""
        qs = QueryStats(
            json_chars=100,
            json_tokens=25,
            toon_chars=40,
            toon_tokens=10,
            tokenizer_name="gpt-4",
            query_type="query"
        )
        self.assertEqual(qs.savings_chars_percent, 60.0)  # (100-40)/100 * 100
    
    def test_savings_tokens_percent(self):
        """Test savings_tokens_percent calculation"""
        qs = QueryStats(
            json_chars=100,
            json_tokens=25,
            toon_chars=40,
            toon_tokens=10,
            tokenizer_name="gpt-4",
            query_type="query"
        )
        self.assertEqual(qs.savings_tokens_percent, 60.0)  # (25-10)/25 * 100
    
    def test_chars_saved(self):
        """Test chars_saved calculation"""
        qs = QueryStats(
            json_chars=100,
            json_tokens=25,
            toon_chars=40,
            toon_tokens=10,
            tokenizer_name="gpt-4",
            query_type="query"
        )
        self.assertEqual(qs.chars_saved, 60)
    
    def test_tokens_saved(self):
        """Test tokens_saved calculation"""
        qs = QueryStats(
            json_chars=100,
            json_tokens=25,
            toon_chars=40,
            toon_tokens=10,
            tokenizer_name="gpt-4",
            query_type="query"
        )
        self.assertEqual(qs.tokens_saved, 15)
    
    def test_zero_json_chars(self):
        """Test savings calculation with zero JSON chars"""
        qs = QueryStats(
            json_chars=0,
            json_tokens=0,
            toon_chars=0,
            toon_tokens=0,
            tokenizer_name="gpt-4",
            query_type="query"
        )
        self.assertEqual(qs.savings_chars_percent, 0.0)
        self.assertEqual(qs.savings_tokens_percent, 0.0)


class TestSessionStats(unittest.TestCase):
    """Tests for SessionStats class"""
    
    def test_init_disabled(self):
        """Test initialization with stats disabled"""
        session = SessionStats(enabled=False)
        self.assertFalse(session.enabled)
        self.assertEqual(len(session.queries), 0)
    
    def test_init_enabled(self):
        """Test initialization with stats enabled"""
        session = SessionStats(enabled=True, tokenizer_model="gpt-4")
        self.assertTrue(session.enabled)
        self.assertEqual(session.tokenizer_model, "gpt-4")
        self.assertEqual(len(session.queries), 0)
    
    def test_add_query(self):
        """Test adding a query"""
        session = SessionStats(enabled=True)
        session.add_query(
            json_chars=100,
            json_tokens=25,
            toon_chars=40,
            toon_tokens=10,
            query_type="query",
            tokenizer_name="gpt-4"
        )
        self.assertEqual(len(session.queries), 1)
        self.assertEqual(session.queries[0].json_chars, 100)
        self.assertEqual(session.queries[0].toon_chars, 40)
    
    def test_add_query_when_disabled(self):
        """Test that queries are not added when disabled"""
        session = SessionStats(enabled=False)
        session.add_query(
            json_chars=100,
            json_tokens=25,
            toon_chars=40,
            toon_tokens=10,
            query_type="query",
            tokenizer_name="gpt-4"
        )
        self.assertEqual(len(session.queries), 0)
    
    def test_get_formatted_log(self):
        """Test formatted log generation"""
        session = SessionStats(enabled=True)
        qs = QueryStats(
            json_chars=62,
            json_tokens=18,
            toon_chars=21,
            toon_tokens=7,
            tokenizer_name="gpt-4",
            query_type="query"
        )
        log = session.get_formatted_log(qs)
        
        # Check format matches specification
        self.assertIn("[TOON Stats]", log)
        self.assertIn("JSON: 62 chars (18 tokens)", log)
        self.assertIn("TOON: 21 chars (7 tokens)", log)
        self.assertIn("Savings:", log)
        self.assertIn("[gpt-4]", log)
        
        # Check savings percentages are included
        self.assertIn("66.1%", log)  # chars savings: (62-21)/62 * 100 = 66.1%
        self.assertIn("61.1%", log)  # tokens savings: (18-7)/18 * 100 = 61.1%
    
    def test_get_all_logs(self):
        """Test getting all formatted logs"""
        session = SessionStats(enabled=True)
        session.add_query(62, 18, 21, 7, "query", "gpt-4")
        session.add_query(100, 25, 40, 10, "find", "gpt-4")
        
        logs = session.get_all_logs()
        self.assertEqual(len(logs), 2)
        self.assertIn("[TOON Stats]", logs[0])
        self.assertIn("[TOON Stats]", logs[1])
    
    def test_get_summary_empty(self):
        """Test summary with no queries"""
        session = SessionStats(enabled=True)
        summary = session.get_summary()
        
        self.assertEqual(summary["total_queries"], 0)
        self.assertEqual(summary["total_json_chars"], 0)
        self.assertEqual(summary["total_toon_chars"], 0)
    
    def test_get_summary_with_queries(self):
        """Test summary with queries"""
        session = SessionStats(enabled=True)
        session.add_query(100, 25, 40, 10, "query", "gpt-4")
        session.add_query(200, 50, 80, 20, "find", "gpt-4")
        
        summary = session.get_summary()
        
        self.assertEqual(summary["total_queries"], 2)
        self.assertEqual(summary["total_json_chars"], 300)
        self.assertEqual(summary["total_json_tokens"], 75)
        self.assertEqual(summary["total_toon_chars"], 120)
        self.assertEqual(summary["total_toon_tokens"], 30)
        self.assertEqual(summary["total_chars_saved"], 180)
        self.assertEqual(summary["total_tokens_saved"], 45)
        self.assertEqual(summary["queries_by_type"]["query"], 1)
        self.assertEqual(summary["queries_by_type"]["find"], 1)
        self.assertEqual(summary["tokenizer_name"], "gpt-4")
    
    def test_reset(self):
        """Test resetting statistics"""
        session = SessionStats(enabled=True)
        session.add_query(100, 25, 40, 10, "query", "gpt-4")
        self.assertEqual(len(session.queries), 1)
        
        session.reset()
        self.assertEqual(len(session.queries), 0)
    
    def test_to_dict_summary(self):
        """Test to_dict with summary only"""
        session = SessionStats(enabled=True)
        session.add_query(100, 25, 40, 10, "query", "gpt-4")
        
        result = session.to_dict(detailed=False)
        
        self.assertIn("enabled", result)
        self.assertIn("tokenizer_model", result)
        self.assertIn("summary", result)
        self.assertNotIn("queries", result)
    
    def test_to_dict_detailed(self):
        """Test to_dict with detailed=True"""
        session = SessionStats(enabled=True)
        session.add_query(100, 25, 40, 10, "query", "gpt-4")
        
        result = session.to_dict(detailed=True)
        
        self.assertIn("queries", result)
        self.assertEqual(len(result["queries"]), 1)
        self.assertEqual(result["queries"][0]["json_chars"], 100)
        self.assertEqual(result["queries"][0]["toon_chars"], 40)


if __name__ == '__main__':
    unittest.main()

