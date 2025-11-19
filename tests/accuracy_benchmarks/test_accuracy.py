"""
Main accuracy benchmark test runner.
Compares LLM performance with JSON vs TOON format.
"""
import sys
import json
import time
import logging
from typing import Dict, List, Any, Optional
from pathlib import Path
from tqdm import tqdm

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from toonpy import connect, to_toon
from toonpy.core.token_counter import count_tokens

from tests.accuracy_benchmarks.config import BenchmarkConfig
from tests.accuracy_benchmarks.llm_client import GroqLLMClient
from tests.accuracy_benchmarks.metrics import (
    semantic_similarity,
    extraction_accuracy,
    numeric_accuracy,
    information_fidelity,
    exact_match,
    extract_emails
)
from tests.accuracy_benchmarks.test_suite import (
    get_test_cases,
    create_prompt,
    TestCase
)
from tests.accuracy_benchmarks.fixtures.sample_queries import (
    get_postgres_query,
    get_mysql_query,
    get_mongo_query
)
from tests.accuracy_benchmarks.report_generator import ReportGenerator

# Setup minimal logging
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


class AccuracyBenchmark:
    """Main benchmark runner"""
    
    def __init__(
        self,
        db_type: str = "postgresql",
        query_name: str = "small_users",
        llm_client: Optional[GroqLLMClient] = None
    ):
        """
        Initialize benchmark.
        
        Args:
            db_type: Database type ("postgresql", "mysql", "mongodb")
            query_name: Name of query to use from fixtures
            llm_client: LLM client instance (creates new if None)
        """
        BenchmarkConfig.validate()
        BenchmarkConfig.ensure_output_dir()
        
        self.db_type = db_type
        self.query_name = query_name
        self.llm_client = llm_client or GroqLLMClient()
        self.results: List[Dict[str, Any]] = []
        
    def run_benchmark(self) -> Dict[str, Any]:
        """
        Run complete benchmark suite.
        
        Returns:
            Dictionary with benchmark results
        """
        logger.info("=" * 60)
        logger.info("Starting Accuracy Benchmark")
        logger.info("=" * 60)
        logger.info(f"Database: {self.db_type}")
        logger.info(f"Query: {self.query_name}")
        logger.info(f"LLM Model: {self.llm_client.model}")
        logger.info("")
        
        # Get data from database
        logger.info("Step 1/3: Fetching data from database...")
        with tqdm(total=1, desc="Connecting to database", bar_format="{desc}: {elapsed}") as pbar:
            json_data, toon_data, raw_data = self._get_data()
            pbar.update(1)
        
        if not raw_data:
            raise ValueError("No data returned from query. Ensure test database has data.")
        
        logger.info(f"✓ Retrieved {len(raw_data)} rows from database")
        logger.info(f"  JSON size: {len(json_data)} chars")
        logger.info(f"  TOON size: {len(toon_data)} chars")
        logger.info("")
        
        # Get test cases
        test_cases = get_test_cases()
        logger.info(f"Step 2/3: Running {len(test_cases)} test cases...")
        logger.info("")
        
        # Run each test case with progress bar
        with tqdm(total=len(test_cases), desc="Test cases", unit="test") as pbar:
            for test_case in test_cases:
                pbar.set_description(f"Testing: {test_case.name[:30]}")
                result = self._run_test_case(test_case, json_data, toon_data, raw_data)
                self.results.append(result)
                pbar.set_postfix({
                    'Accuracy': f"{result['accuracy']:.1%}",
                    'Savings': f"{result['token_savings']:.1%}"
                })
                pbar.update(1)
        
        logger.info("")
        logger.info("Step 3/3: Generating reports...")
        
        # Generate summary
        summary = self._generate_summary()
        
        logger.info("✓ Benchmark completed!")
        logger.info("")
        
        return {
            'summary': summary,
            'results': self.results,
            'config': {
                'db_type': self.db_type,
                'query_name': self.query_name,
                'model': self.llm_client.model,
                'num_test_cases': len(test_cases)
            }
        }
    
    def _get_data(self) -> tuple[str, str, List[Dict]]:
        """
        Get data from database in both JSON and TOON formats.
        
        Returns:
            Tuple of (json_string, toon_string, raw_data)
        """
        from toonpy.core.converter import from_toon
        
        logger.info(f"  Connecting to {self.db_type}...")
        
        # Connect to database
        if self.db_type == "postgresql":
            adapter = connect(BenchmarkConfig.POSTGRES_CONN_STRING)
            query = get_postgres_query(self.query_name)
            logger.info(f"  Executing query: {self.query_name}")
            # Get TOON format
            toon_data = adapter.query(query)
            # Convert TOON back to raw data
            raw_data = from_toon(toon_data)
        elif self.db_type == "mysql":
            adapter = connect(BenchmarkConfig.MYSQL_CONN_STRING)
            query = get_mysql_query(self.query_name)
            logger.info(f"  Executing query: {self.query_name}")
            # Get TOON format
            toon_data = adapter.query(query)
            # Convert TOON back to raw data
            raw_data = from_toon(toon_data)
        elif self.db_type == "mongodb":
            adapter = connect(
                BenchmarkConfig.MONGO_CONN_STRING,
                database=BenchmarkConfig.MONGO_DATABASE,
                collection_name="users"
            )
            mongo_query = get_mongo_query(self.query_name)
            logger.info(f"  Executing MongoDB query: {self.query_name}")
            # Get TOON format
            toon_data = adapter.find(mongo_query["query"], mongo_query.get("projection"))
            # Convert TOON back to raw data
            raw_data = from_toon(toon_data)
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")
        
        adapter.close()
        
        # Convert raw_data to JSON string
        json_string = json.dumps(raw_data, separators=(',', ':'))
        
        return json_string, toon_data, raw_data
    
    def _run_test_case(
        self,
        test_case: TestCase,
        json_data: str,
        toon_data: str,
        raw_data: List[Dict]
    ) -> Dict[str, Any]:
        """
        Run a single test case with both JSON and TOON formats.
        
        Args:
            test_case: Test case definition
            json_data: Data in JSON format
            toon_data: Data in TOON format
            raw_data: Raw data for ground truth extraction
        
        Returns:
            Result dictionary with accuracy metrics
        """
        # Create prompts
        json_prompt = create_prompt(test_case.task, json_data, "JSON")
        toon_prompt = create_prompt(test_case.task, toon_data, "TOON")
        
        # Get ground truth if needed
        ground_truth = self._extract_ground_truth(test_case, raw_data)
        
        # Run with JSON format
        with tqdm(total=2, desc=f"  {test_case.name[:25]}", leave=False, bar_format="{desc}: {n_fmt}/{total_fmt}") as pbar:
            pbar.set_description(f"  {test_case.name[:25]} [JSON]")
            json_start = time.time()
            json_response = self.llm_client.chat(json_prompt)
            json_latency = time.time() - json_start
            pbar.update(1)
            
            # Run with TOON format
            pbar.set_description(f"  {test_case.name[:25]} [TOON]")
            toon_start = time.time()
            toon_response = self.llm_client.chat(toon_prompt)
            toon_latency = time.time() - toon_start
            pbar.update(1)
        
        # Calculate accuracy based on evaluation method
        accuracy = self._calculate_accuracy(
            test_case,
            json_response['response'],
            toon_response['response'],
            ground_truth
        )
        
        # Calculate token savings
        json_tokens = json_response['tokens_used']
        toon_tokens = toon_response['tokens_used']
        token_savings = (json_tokens - toon_tokens) / json_tokens if json_tokens > 0 else 0.0
        
        # Calculate latency difference
        latency_diff = toon_latency - json_latency
        
        return {
            'test_name': test_case.name,
            'task': test_case.task,
            'evaluation_method': test_case.evaluation_method,
            'accuracy': accuracy,
            'json_response': json_response['response'],
            'toon_response': toon_response['response'],
            'json_tokens': json_tokens,
            'toon_tokens': toon_tokens,
            'token_savings': token_savings,
            'json_latency': json_latency,
            'toon_latency': toon_latency,
            'latency_diff': latency_diff,
            'ground_truth': ground_truth
        }
    
    def _extract_ground_truth(self, test_case: TestCase, raw_data: List[Dict]) -> Any:
        """Extract ground truth from raw data based on test case"""
        if test_case.field == "emails":
            # Extract emails from raw data
            emails = set()
            for row in raw_data:
                if 'email' in row and row['email']:
                    emails.add(row['email'].lower())
            return list(emails)
        elif test_case.evaluation_method == "numeric" and "average" in test_case.task.lower():
            # Calculate average age
            ages = [row.get('age') for row in raw_data if row.get('age') is not None]
            if ages:
                return sum(ages) / len(ages)
        elif test_case.evaluation_method == "numeric" and "how many" in test_case.task.lower():
            # Count based on condition
            if "role='user'" in test_case.task or "role=\"user\"" in test_case.task:
                return sum(1 for row in raw_data if row.get('role') == 'user')
        elif "older than 30" in test_case.task.lower() or "age > 30" in test_case.task.lower():
            # For filtering tasks, return expected names (age > 30, not >= 30)
            expected = []
            for row in raw_data:
                age = row.get('age')
                if age is not None and age > 30:  # Strictly greater than 30
                    expected.append({
                        'name': row.get('name', ''),
                        'age': age
                    })
            return expected
        return None
    
    def _normalize_response(self, text: str) -> str:
        """Normalize response by removing code blocks and extra formatting"""
        import re
        # Remove code blocks
        text = re.sub(r'```[\s\S]*?```', '', text)
        # Remove markdown code blocks
        text = re.sub(r'`[^`]+`', '', text)
        # Remove common prefixes
        prefixes = [
            r'^Here are the .+?:?\s*',
            r'^Based on the .+?:?\s*',
            r'^To .+?:?\s*',
            r'^The .+? are:\s*',
            r'^Here\'s .+?:?\s*',
        ]
        for prefix in prefixes:
            text = re.sub(prefix, '', text, flags=re.IGNORECASE | re.MULTILINE)
        # Remove leading/trailing whitespace and newlines
        text = text.strip()
        return text
    
    def _extract_numeric_answer(self, text: str) -> Optional[float]:
        """Extract numeric answer from text, handling code blocks and explanations"""
        import re
        # First, try to find standalone numbers (most likely the answer)
        # Look for numbers that appear after common phrases or at the end
        patterns = [
            r'(?:average|result|answer|total|count|number|is|are|:)\s*([0-9]+\.?[0-9]*)',
            r'^([0-9]+\.?[0-9]*)\s*$',  # Line with just a number
            r'([0-9]+\.?[0-9]*)',  # Any number as fallback
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                try:
                    # Return the first reasonable number (not an ID or year)
                    for match in matches:
                        num = float(match)
                        # Filter out unlikely answers (IDs, years, etc.)
                        if 0 <= num <= 200:  # Reasonable range for ages/counts
                            return num
                except ValueError:
                    continue
        
        return None
    
    def _calculate_accuracy(
        self,
        test_case: TestCase,
        json_output: str,
        toon_output: str,
        ground_truth: Any
    ) -> float:
        """Calculate accuracy based on evaluation method"""
        # Normalize responses for better comparison
        json_normalized = self._normalize_response(json_output)
        toon_normalized = self._normalize_response(toon_output)
        
        if test_case.evaluation_method == "semantic_similarity":
            # Use normalized versions for comparison
            return semantic_similarity(json_normalized, toon_normalized)
        elif test_case.evaluation_method == "extraction":
            if ground_truth and test_case.field:
                # Compare both against ground truth
                json_acc = extraction_accuracy(json_normalized, ground_truth, test_case.field)
                toon_acc = extraction_accuracy(toon_normalized, ground_truth, test_case.field)
                # Return average or minimum (conservative)
                return min(json_acc, toon_acc)  # Conservative: both must be accurate
            else:
                return semantic_similarity(json_normalized, toon_normalized)
        elif test_case.evaluation_method == "numeric":
            tolerance = test_case.tolerance or 0.01
            
            # Try to extract numeric answers first
            json_num = self._extract_numeric_answer(json_output)
            toon_num = self._extract_numeric_answer(toon_output)
            
            if json_num is not None and toon_num is not None:
                # Both extracted numbers - compare directly
                if abs(json_num - toon_num) <= tolerance:
                    return 1.0
                else:
                    # Calculate similarity based on how close they are
                    diff = abs(json_num - toon_num)
                    max_val = max(abs(json_num), abs(toon_num), 1.0)
                    similarity = max(0.0, 1.0 - (diff / max_val))
                    return similarity
            else:
                # Fall back to text-based numeric comparison
                return numeric_accuracy(json_normalized, toon_normalized, tolerance)
        elif test_case.evaluation_method == "exact_match":
            return exact_match(json_normalized, toon_normalized)
        else:
            # Default to semantic similarity
            return semantic_similarity(json_normalized, toon_normalized)
    
    def _generate_summary(self) -> Dict[str, Any]:
        """Generate summary statistics from all results"""
        if not self.results:
            return {}
        
        accuracies = [r['accuracy'] for r in self.results]
        token_savings = [r['token_savings'] for r in self.results]
        latency_diffs = [r['latency_diff'] for r in self.results]
        
        return {
            'average_accuracy': sum(accuracies) / len(accuracies),
            'min_accuracy': min(accuracies),
            'max_accuracy': max(accuracies),
            'average_token_savings': sum(token_savings) / len(token_savings),
            'total_token_savings': sum(r['json_tokens'] - r['toon_tokens'] for r in self.results),
            'average_latency_diff': sum(latency_diffs) / len(latency_diffs),
            'num_tests': len(self.results)
        }


def main():
    """Main entry point for running benchmarks"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Run accuracy benchmarks")
    parser.add_argument(
        "--db-type",
        choices=["postgresql", "mysql", "mongodb"],
        default="postgresql",
        help="Database type to use"
    )
    parser.add_argument(
        "--query",
        default="small_users",
        help="Query name from fixtures"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        help="Output directory for reports (defaults to benchmark_results/)"
    )
    
    args = parser.parse_args()
    
    if args.output_dir:
        BenchmarkConfig.OUTPUT_DIR = Path(args.output_dir)
        BenchmarkConfig.ensure_output_dir()
    
    # Run benchmark
    benchmark = AccuracyBenchmark(
        db_type=args.db_type,
        query_name=args.query
    )
    
    results = benchmark.run_benchmark()
    
    # Generate reports
    logger.info("  Generating reports...")
    with tqdm(total=4, desc="Generating reports", bar_format="{desc}: {n_fmt}/{total_fmt}") as pbar:
        generator = ReportGenerator(results, BenchmarkConfig.OUTPUT_DIR)
        generator.generate_console()
        pbar.update(1)
        generator.generate_json()
        pbar.update(1)
        generator.generate_markdown()
        pbar.update(1)
        generator.generate_csv()
        pbar.update(1)
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("Benchmark completed!")
    logger.info(f"Reports saved to: {BenchmarkConfig.OUTPUT_DIR}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

