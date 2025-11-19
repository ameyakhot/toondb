"""
Calculate actual cost savings from token reduction.
Shows real-world dollar impact of using TOON format.
"""
import unittest
import json
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from toonpy import to_toon
from toonpy.core.token_counter import count_tokens

# LLM pricing per 1K tokens (update with current prices)
# Source: OpenAI pricing as of 2024
PRICING = {
    "gpt-4": {
        "input": 0.03,   # $0.03 per 1K input tokens
        "output": 0.06,  # $0.06 per 1K output tokens
    },
    "gpt-4-turbo": {
        "input": 0.01,
        "output": 0.03,
    },
    "gpt-3.5-turbo": {
        "input": 0.0015,  # $0.0015 per 1K input tokens
        "output": 0.002,  # $0.002 per 1K output tokens
    },
    "gpt-4o": {
        "input": 0.005,
        "output": 0.015,
    },
}


class TestCostSavings(unittest.TestCase):
    """Test actual cost savings calculations"""
    
    def test_calculate_savings_small_query(self):
        """Calculate savings for a small query (10 rows)"""
        data = [{"id": i, "name": f"User{i}", "age": 20+i, "email": f"user{i}@test.com"}
                for i in range(10)]
        
        json_str = json.dumps(data, separators=(',', ':'))
        toon_str = to_toon(data)
        
        model = "gpt-4"
        json_tokens = count_tokens(json_str, model=model)
        toon_tokens = count_tokens(toon_str, model=model)
        
        tokens_saved = json_tokens - toon_tokens
        cost_per_1k = PRICING[model]["input"]
        savings_per_query = (tokens_saved / 1000) * cost_per_1k
        
        print(f"\n{'='*60}")
        print(f"Cost Savings Analysis - Small Query (10 rows)")
        print(f"{'='*60}")
        print(f"Model: {model}")
        print(f"JSON tokens: {json_tokens:,}")
        print(f"TOON tokens: {toon_tokens:,}")
        print(f"Tokens saved: {tokens_saved:,} ({tokens_saved/json_tokens*100:.1f}%)")
        print(f"Cost per 1K tokens: ${cost_per_1k:.4f}")
        print(f"Savings per query: ${savings_per_query:.6f}")
        print(f"{'='*60}")
        
        self.assertGreater(savings_per_query, 0)
    
    def test_calculate_savings_medium_query(self):
        """Calculate savings for a medium query (100 rows)"""
        data = [{"id": i, "name": f"User{i}", "age": 20+i, "email": f"user{i}@test.com",
                 "role": "user" if i % 2 == 0 else "admin", "active": True}
                for i in range(100)]
        
        json_str = json.dumps(data, separators=(',', ':'))
        toon_str = to_toon(data)
        
        model = "gpt-4"
        json_tokens = count_tokens(json_str, model=model)
        toon_tokens = count_tokens(toon_str, model=model)
        
        tokens_saved = json_tokens - toon_tokens
        cost_per_1k = PRICING[model]["input"]
        savings_per_query = (tokens_saved / 1000) * cost_per_1k
        
        print(f"\n{'='*60}")
        print(f"Cost Savings Analysis - Medium Query (100 rows)")
        print(f"{'='*60}")
        print(f"Model: {model}")
        print(f"JSON tokens: {json_tokens:,}")
        print(f"TOON tokens: {toon_tokens:,}")
        print(f"Tokens saved: {tokens_saved:,} ({tokens_saved/json_tokens*100:.1f}%)")
        print(f"Savings per query: ${savings_per_query:.6f}")
        print(f"{'='*60}")
        
        self.assertGreater(savings_per_query, 0)
    
    def test_calculate_savings_large_query(self):
        """Calculate savings for a large query (500 rows)"""
        data = [{"id": i, "name": f"User{i}", "age": 20+(i%50), "email": f"user{i}@test.com",
                 "role": "user" if i % 3 == 0 else "admin", "active": i % 2 == 0,
                 "created_at": "2024-01-01", "last_login": "2024-11-19"}
                for i in range(500)]
        
        json_str = json.dumps(data, separators=(',', ':'))
        toon_str = to_toon(data)
        
        model = "gpt-4"
        json_tokens = count_tokens(json_str, model=model)
        toon_tokens = count_tokens(toon_str, model=model)
        
        tokens_saved = json_tokens - toon_tokens
        cost_per_1k = PRICING[model]["input"]
        savings_per_query = (tokens_saved / 1000) * cost_per_1k
        
        print(f"\n{'='*60}")
        print(f"Cost Savings Analysis - Large Query (500 rows)")
        print(f"{'='*60}")
        print(f"Model: {model}")
        print(f"JSON tokens: {json_tokens:,}")
        print(f"TOON tokens: {toon_tokens:,}")
        print(f"Tokens saved: {tokens_saved:,} ({tokens_saved/json_tokens*100:.1f}%)")
        print(f"Savings per query: ${savings_per_query:.6f}")
        print(f"{'='*60}")
        
        self.assertGreater(savings_per_query, 0)
    
    def test_monthly_savings_projection(self):
        """Project monthly savings for high-volume scenarios"""
        data = [{"id": i, "name": f"User{i}", "age": 20+i, "email": f"user{i}@test.com"}
                for i in range(50)]
        
        json_str = json.dumps(data, separators=(',', ':'))
        toon_str = to_toon(data)
        
        model = "gpt-4"
        json_tokens = count_tokens(json_str, model=model)
        toon_tokens = count_tokens(toon_str, model=model)
        
        tokens_saved = json_tokens - toon_tokens
        cost_per_1k = PRICING[model]["input"]
        savings_per_query = (tokens_saved / 1000) * cost_per_1k
        
        # Different volume scenarios
        scenarios = [
            ("Low Volume", 1_000),
            ("Medium Volume", 10_000),
            ("High Volume", 100_000),
            ("Enterprise", 1_000_000),
        ]
        
        print(f"\n{'='*60}")
        print(f"Monthly Savings Projections")
        print(f"{'='*60}")
        print(f"Model: {model}")
        print(f"Savings per query: ${savings_per_query:.6f}")
        print(f"\nVolume Scenarios:")
        
        for scenario_name, queries_per_month in scenarios:
            monthly_savings = savings_per_query * queries_per_month
            annual_savings = monthly_savings * 12
            
            print(f"\n  {scenario_name} ({queries_per_month:,} queries/month):")
            print(f"    Monthly: ${monthly_savings:,.2f}")
            print(f"    Annual:  ${annual_savings:,.2f}")
        
        print(f"{'='*60}")
    
    def test_savings_across_models(self):
        """Compare savings across different LLM models"""
        data = [{"id": i, "name": f"User{i}", "age": 20+i, "email": f"user{i}@test.com"}
                for i in range(50)]
        
        json_str = json.dumps(data, separators=(',', ':'))
        toon_str = to_toon(data)
        
        print(f"\n{'='*60}")
        print(f"Cost Savings Across Models (50 rows)")
        print(f"{'='*60}")
        
        results = []
        for model in PRICING.keys():
            try:
                json_tokens = count_tokens(json_str, model=model)
                toon_tokens = count_tokens(toon_str, model=model)
                tokens_saved = json_tokens - toon_tokens
                savings_pct = (tokens_saved / json_tokens * 100) if json_tokens > 0 else 0
                
                cost_per_1k = PRICING[model]["input"]
                savings_per_query = (tokens_saved / 1000) * cost_per_1k
                monthly_10k = savings_per_query * 10_000
                
                results.append({
                    "model": model,
                    "tokens_saved": tokens_saved,
                    "savings_pct": savings_pct,
                    "savings_per_query": savings_per_query,
                    "monthly_10k": monthly_10k
                })
                
                print(f"\n{model}:")
                print(f"  Tokens saved: {tokens_saved:,} ({savings_pct:.1f}%)")
                print(f"  Savings per query: ${savings_per_query:.6f}")
                print(f"  Monthly (10K queries): ${monthly_10k:,.2f}")
            except Exception as e:
                print(f"\n{model}: Error - {e}")
        
        print(f"{'='*60}")
        
        # Verify all models show savings
        for result in results:
            self.assertGreater(result["savings_per_query"], 0)
    
    def test_roi_calculation(self):
        """Calculate ROI for adopting TOON format"""
        # Assumptions
        implementation_hours = 4  # Time to integrate TOON
        hourly_rate = 100  # Developer hourly rate
        implementation_cost = implementation_hours * hourly_rate
        
        # Calculate ongoing savings
        data = [{"id": i, "name": f"User{i}", "age": 20+i, "email": f"user{i}@test.com"} 
                for i in range(50)]
        json_str = json.dumps(data, separators=(',', ':'))
        toon_str = to_toon(data)
        
        model = "gpt-4"
        json_tokens = count_tokens(json_str, model=model)
        toon_tokens = count_tokens(toon_str, model=model)
        tokens_saved = json_tokens - toon_tokens
        savings_per_query = (tokens_saved / 1000) * PRICING[model]["input"]
        
        queries_per_month = 10_000
        monthly_savings = savings_per_query * queries_per_month
        
        # Break-even point
        months_to_break_even = implementation_cost / monthly_savings if monthly_savings > 0 else float('inf')
        
        print(f"\n{'='*60}")
        print(f"ROI Analysis")
        print(f"{'='*60}")
        print(f"Implementation Cost:")
        print(f"  Hours: {implementation_hours}")
        print(f"  Rate: ${hourly_rate}/hour")
        print(f"  Total: ${implementation_cost}")
        print(f"\nOngoing Savings:")
        print(f"  Savings per query: ${savings_per_query:.6f}")
        print(f"  Queries/month: {queries_per_month:,}")
        print(f"  Monthly savings: ${monthly_savings:,.2f}")
        print(f"\nBreak-even:")
        print(f"  Months to break-even: {months_to_break_even:.1f}")
        if months_to_break_even < 12:
            print(f"  [PASS] ROI positive within first year!")
        else:
            print(f"  [WARN] Break-even in {months_to_break_even:.1f} months")
        print(f"{'='*60}")
        
        # For reasonable volumes, ROI should be positive
        if queries_per_month >= 1000:
            self.assertLess(months_to_break_even, 12, "Should break even within a year for 10K+ queries/month")
    
    def test_cost_comparison_table(self):
        """Generate a comprehensive cost comparison table"""
        data_sizes = [
            ("Small (10 rows)", 10),
            ("Medium (50 rows)", 50),
            ("Large (100 rows)", 100),
            ("Very Large (500 rows)", 500),
        ]
        
        print(f"\n{'='*80}")
        print(f"Cost Savings Comparison Table")
        print(f"{'='*80}")
        print(f"{'Data Size':<20} {'Model':<15} {'JSON Tokens':<15} {'TOON Tokens':<15} {'Savings':<15}")
        print(f"{'-'*80}")
        
        for size_name, row_count in data_sizes:
            data = [{"id": i, "name": f"User{i}", "age": 20+i, "email": f"user{i}@test.com"}
                    for i in range(row_count)]
            json_str = json.dumps(data, separators=(',', ':'))
            toon_str = to_toon(data)
            
            for model in ["gpt-4", "gpt-3.5-turbo"]:
                try:
                    json_tokens = count_tokens(json_str, model=model)
                    toon_tokens = count_tokens(toon_str, model=model)
                    tokens_saved = json_tokens - toon_tokens
                    savings_pct = (tokens_saved / json_tokens * 100) if json_tokens > 0 else 0
                    
                    cost_per_1k = PRICING[model]["input"]
                    savings_per_query = (tokens_saved / 1000) * cost_per_1k
                    
                    print(f"{size_name:<20} {model:<15} {json_tokens:<15,} {toon_tokens:<15,} "
                          f"${savings_per_query:.6f} ({savings_pct:.1f}%)")
                except Exception:
                    pass
        
        print(f"{'='*80}")


if __name__ == '__main__':
    unittest.main(verbosity=2)

