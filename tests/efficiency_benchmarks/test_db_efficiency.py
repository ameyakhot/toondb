"""
Database-specific efficiency tests using real database connections.
Tests token efficiency with actual database queries.
"""
import unittest
import json
import os
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from toonpy import connect
from toonpy.core.converter import from_toon
from toonpy.core.token_counter import count_tokens, count_chars


class TestDatabaseEfficiency(unittest.TestCase):
    """Test token efficiency with real database queries"""
    
    @classmethod
    def setUpClass(cls):
        """Set up database connections"""
        # Load from environment or use defaults
        cls.postgres_conn = os.getenv(
            "POSTGRES_CONN_STRING",
            "postgresql://testuser:testpass@localhost:5433/testdb"
        )
        cls.mysql_conn = os.getenv(
            "MYSQL_CONN_STRING",
            "mysql://testuser:testpass@localhost:3307/testdb"
        )
        cls.mongo_conn = os.getenv(
            "MONGO_CONN_STRING",
            "mongodb://localhost:27017"
        )
        cls.mongo_db = os.getenv("MONGO_DATABASE", "testdb")
    
    def test_postgresql_efficiency(self):
        """Test PostgreSQL query efficiency across different sizes"""
        try:
            adapter = connect(self.postgres_conn)
            
            queries = [
                ("SELECT * FROM users LIMIT 1", "single_row"),
                ("SELECT * FROM users LIMIT 10", "small"),
                ("SELECT * FROM users LIMIT 50", "medium"),
                ("SELECT * FROM users LIMIT 100", "large"),
            ]
            
            print(f"\n{'='*60}")
            print(f"PostgreSQL Efficiency Test")
            print(f"{'='*60}")
            
            for query, size in queries:
                try:
                    toon_result = adapter.query(query)
                    raw_data = from_toon(toon_result)
                    json_str = json.dumps(raw_data, separators=(',', ':'))
                    
                    json_tokens = count_tokens(json_str)
                    toon_tokens = count_tokens(toon_result)
                    json_chars = count_chars(json_str)
                    toon_chars = count_chars(toon_result)
                    
                    token_savings = (json_tokens - toon_tokens) / json_tokens * 100 if json_tokens > 0 else 0
                    char_savings = (json_chars - toon_chars) / json_chars * 100 if json_chars > 0 else 0
                    
                    print(f"\n{size} ({len(raw_data)} rows):")
                    print(f"  JSON: {json_chars:,} chars, {json_tokens:,} tokens")
                    print(f"  TOON: {toon_chars:,} chars, {toon_tokens:,} tokens")
                    print(f"  Savings: {char_savings:.1f}% chars, {token_savings:.1f}% tokens")
                    
                    if len(raw_data) > 1:
                        self.assertGreater(token_savings, 20.0, 
                                          f"Should save tokens for {size} query")
                except Exception as e:
                    print(f"  Error with {size}: {e}")
            
            adapter.close()
        except Exception as e:
            self.skipTest(f"PostgreSQL not available: {e}")
    
    def test_mysql_efficiency(self):
        """Test MySQL query efficiency across different sizes"""
        try:
            adapter = connect(self.mysql_conn)
            
            queries = [
                ("SELECT * FROM users LIMIT 1", "single_row"),
                ("SELECT * FROM users LIMIT 10", "small"),
                ("SELECT * FROM users LIMIT 50", "medium"),
                ("SELECT * FROM users LIMIT 100", "large"),
            ]
            
            print(f"\n{'='*60}")
            print(f"MySQL Efficiency Test")
            print(f"{'='*60}")
            
            for query, size in queries:
                try:
                    toon_result = adapter.query(query)
                    raw_data = from_toon(toon_result)
                    json_str = json.dumps(raw_data, separators=(',', ':'))
                    
                    json_tokens = count_tokens(json_str)
                    toon_tokens = count_tokens(toon_result)
                    json_chars = count_chars(json_str)
                    toon_chars = count_chars(toon_result)
                    
                    token_savings = (json_tokens - toon_tokens) / json_tokens * 100 if json_tokens > 0 else 0
                    char_savings = (json_chars - toon_chars) / json_chars * 100 if json_chars > 0 else 0
                    
                    print(f"\n{size} ({len(raw_data)} rows):")
                    print(f"  JSON: {json_chars:,} chars, {json_tokens:,} tokens")
                    print(f"  TOON: {toon_chars:,} chars, {toon_tokens:,} tokens")
                    print(f"  Savings: {char_savings:.1f}% chars, {token_savings:.1f}% tokens")
                    
                    if len(raw_data) > 1:
                        self.assertGreater(token_savings, 20.0, 
                                          f"Should save tokens for {size} query")
                except Exception as e:
                    print(f"  Error with {size}: {e}")
            
            adapter.close()
        except Exception as e:
            self.skipTest(f"MySQL not available: {e}")
    
    def test_mongodb_efficiency(self):
        """Test MongoDB query efficiency"""
        try:
            adapter = connect(
                self.mongo_conn,
                database=self.mongo_db,
                collection_name="users"
            )
            
            queries = [
                (1, "single_row"),
                (10, "small"),
                (50, "medium"),
                (100, "large"),
            ]
            
            print(f"\n{'='*60}")
            print(f"MongoDB Efficiency Test")
            print(f"{'='*60}")
            
            for limit, size in queries:
                try:
                    # MongoDB find - need to limit results manually via cursor
                    # Get results and limit manually
                    cursor = adapter.collection.find({}).limit(limit)
                    mongo_results = list(cursor)
                    # Convert to TOON format
                    toon_result = adapter._to_toon(adapter._clean_mongo_docs(mongo_results))
                    raw_data = from_toon(toon_result)
                    json_str = json.dumps(raw_data, separators=(',', ':'))
                    
                    json_tokens = count_tokens(json_str)
                    toon_tokens = count_tokens(toon_result)
                    json_chars = count_chars(json_str)
                    toon_chars = count_chars(toon_result)
                    
                    token_savings = (json_tokens - toon_tokens) / json_tokens * 100 if json_tokens > 0 else 0
                    char_savings = (json_chars - toon_chars) / json_chars * 100 if json_chars > 0 else 0
                    
                    print(f"\n{size} ({len(raw_data)} rows):")
                    print(f"  JSON: {json_chars:,} chars, {json_tokens:,} tokens")
                    print(f"  TOON: {toon_chars:,} chars, {toon_tokens:,} tokens")
                    print(f"  Savings: {char_savings:.1f}% chars, {token_savings:.1f}% tokens")
                    
                    if len(raw_data) > 1:
                        self.assertGreater(token_savings, 20.0, 
                                          f"Should save tokens for {size} query")
                except Exception as e:
                    print(f"  Error with {size}: {e}")
            
            adapter.close()
        except Exception as e:
            self.skipTest(f"MongoDB not available: {e}")
    
    def test_cross_database_comparison(self):
        """Compare efficiency across different databases for similar data"""
        print(f"\n{'='*60}")
        print(f"Cross-Database Efficiency Comparison")
        print(f"{'='*60}")
        
        results = {}
        
        # Test PostgreSQL
        try:
            adapter = connect(self.postgres_conn)
            toon_result = adapter.query("SELECT * FROM users LIMIT 20")
            raw_data = from_toon(toon_result)
            json_str = json.dumps(raw_data, separators=(',', ':'))
            
            json_tokens = count_tokens(json_str)
            toon_tokens = count_tokens(toon_result)
            token_savings = (json_tokens - toon_tokens) / json_tokens * 100 if json_tokens > 0 else 0
            
            results["PostgreSQL"] = {
                "rows": len(raw_data),
                "json_tokens": json_tokens,
                "toon_tokens": toon_tokens,
                "savings": token_savings
            }
            adapter.close()
        except Exception as e:
            print(f"PostgreSQL: Not available ({e})")
        
        # Test MySQL
        try:
            adapter = connect(self.mysql_conn)
            toon_result = adapter.query("SELECT * FROM users LIMIT 20")
            raw_data = from_toon(toon_result)
            json_str = json.dumps(raw_data, separators=(',', ':'))
            
            json_tokens = count_tokens(json_str)
            toon_tokens = count_tokens(toon_result)
            token_savings = (json_tokens - toon_tokens) / json_tokens * 100 if json_tokens > 0 else 0
            
            results["MySQL"] = {
                "rows": len(raw_data),
                "json_tokens": json_tokens,
                "toon_tokens": toon_tokens,
                "savings": token_savings
            }
            adapter.close()
        except Exception as e:
            print(f"MySQL: Not available ({e})")
        
        # Test MongoDB
        try:
            adapter = connect(
                self.mongo_conn,
                database=self.mongo_db,
                collection_name="users"
            )
            # Get 20 results using cursor limit
            cursor = adapter.collection.find({}).limit(20)
            mongo_results = list(cursor)
            toon_result = adapter._to_toon(adapter._clean_mongo_docs(mongo_results))
            raw_data = from_toon(toon_result)
            json_str = json.dumps(raw_data, separators=(',', ':'))
            
            json_tokens = count_tokens(json_str)
            toon_tokens = count_tokens(toon_result)
            token_savings = (json_tokens - toon_tokens) / json_tokens * 100 if json_tokens > 0 else 0
            
            results["MongoDB"] = {
                "rows": len(raw_data),
                "json_tokens": json_tokens,
                "toon_tokens": toon_tokens,
                "savings": token_savings
            }
            adapter.close()
        except Exception as e:
            print(f"MongoDB: Not available ({e})")
        
        # Print comparison
        if results:
            print(f"\n{'Database':<15} {'Rows':<10} {'JSON Tokens':<15} {'TOON Tokens':<15} {'Savings':<15}")
            print(f"{'-'*70}")
            for db_name, result in results.items():
                print(f"{db_name:<15} {result['rows']:<10} {result['json_tokens']:<15,} "
                      f"{result['toon_tokens']:<15,} {result['savings']:.1f}%")
            print(f"{'='*60}")


if __name__ == '__main__':
    unittest.main(verbosity=2)

