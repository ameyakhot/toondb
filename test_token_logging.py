"""
Test script to demonstrate token logging functionality
"""
import tempfile
import os
from toonpy.adapters.base import BaseAdapter
from unittest.mock import Mock

# Create a test adapter
class TestAdapter(BaseAdapter):
    def query(self, query): return ''
    def close(self): pass

# Create temp log file
with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.log') as f:
    log_file = f.name

try:
    # Create adapter with verbose and log file
    print("Creating adapter with verbose=True and log_file specified...")
    adapter = TestAdapter(verbose=True, log_file=log_file, tokenizer_model='gpt-4')
    
    # Run some queries to generate logs
    print("\nRunning test queries...")
    test_data = [
        {'name': 'John Doe', 'age': 30, 'city': 'New York', 'email': 'john@example.com'},
        {'name': 'Jane Smith', 'age': 25, 'city': 'Boston', 'email': 'jane@example.com'},
        {'name': 'Bob Johnson', 'age': 35, 'city': 'Chicago', 'email': 'bob@example.com'}
    ]
    
    # Simulate different query types
    print("  - Running find query...")
    adapter._to_toon(test_data, query_type='find')
    
    print("  - Running find_one query...")
    adapter._to_toon([test_data[0]], query_type='find_one')
    
    print("  - Running aggregate query...")
    adapter._to_toon(test_data[:2], query_type='aggregate')
    
    print("  - Running query...")
    adapter._to_toon(test_data, query_type='query')
    
    # Read and display the log file
    print("\n" + "="*80)
    print("TOKEN LOGS (from file):")
    print("="*80)
    if os.path.exists(log_file):
        with open(log_file, 'r') as f:
            content = f.read()
            print(content)
    else:
        print('Log file not found')
    
    # Also show stats summary
    print("\n" + "="*80)
    print("STATISTICS SUMMARY:")
    print("="*80)
    adapter.print_stats()
        
finally:
    # Clean up
    if os.path.exists(log_file):
        print(f"\nLog file was saved to: {log_file}")
        # Don't delete so user can see it
        # os.unlink(log_file)

