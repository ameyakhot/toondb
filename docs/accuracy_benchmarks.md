# Accuracy Benchmarks

This document describes how to run accuracy benchmarks to compare LLM performance when using JSON vs TOON format for database query results.

## Overview

The accuracy benchmark suite measures whether TOON format maintains equivalent accuracy to JSON format while reducing token usage. It runs the same tasks with both formats and compares:

- **Semantic Similarity**: Do the outputs mean the same thing?
- **Extraction Accuracy**: Can the LLM extract the same information?
- **Numeric Accuracy**: Are numeric calculations consistent?
- **Token Savings**: How many tokens are saved?
- **Latency**: Is there a performance difference?

## Prerequisites

1. **Groq API Key**: Set `GROQ_API_KEY` in `.env` file at project root
2. **Test Database**: PostgreSQL, MySQL, or MongoDB with test data
3. **Dependencies**: Install required packages (see below)

## Installation

Install benchmark dependencies:

```bash
pip install python-dotenv sentence-transformers numpy
```

Or if using the project's requirements:

```bash
pip install -r requirements.txt
```

## Configuration

Create or update `.env` file in project root (`A:\toondb\.env`):

```env
GROQ_API_KEY=your_groq_api_key_here
GROQ_MODEL=llama-3.1-70b-versatile
BENCHMARK_TEMPERATURE=0.0
BENCHMARK_MAX_TOKENS=1000
BENCHMARK_NUM_RUNS=1

# Database connection strings (optional, defaults provided)
POSTGRES_CONN_STRING=postgresql://testuser:testpass@localhost:5433/testdb
MYSQL_CONN_STRING=mysql://testuser:testpass@localhost:3307/testdb
MONGO_CONN_STRING=mongodb://localhost:27017
MONGO_DATABASE=testdb
```

### Available Groq Models

- `llama-3.1-8b-instant` (default - fast and reliable)
- `llama-3.3-70b-versatile` (if available - successor to 3.1-70b)
- `mixtral-8x7b-32768`
- `gemma-7b-it`

## Running Benchmarks

### Basic Usage

Run benchmarks with default settings (PostgreSQL, small_users query):

```bash
python -m tests.accuracy_benchmarks.test_accuracy
```

### Command Line Options

```bash
python -m tests.accuracy_benchmarks.test_accuracy \
    --db-type postgresql \
    --query small_users \
    --output-dir ./my_results
```

**Options:**
- `--db-type`: Database type (`postgresql`, `mysql`, `mongodb`)
- `--query`: Query name from fixtures (`small_users`, `medium_users`, `aggregated_stats`, `filtered_users`)
- `--output-dir`: Output directory for reports (defaults to `benchmark_results/`)

### Using as Python Module

```python
from tests.accuracy_benchmarks.test_accuracy import AccuracyBenchmark
from tests.accuracy_benchmarks.llm_client import GroqLLMClient

# Create LLM client
llm_client = GroqLLMClient()

# Run benchmark
benchmark = AccuracyBenchmark(
    db_type="postgresql",
    query_name="small_users",
    llm_client=llm_client
)

results = benchmark.run_benchmark()

# Access results
print(f"Average Accuracy: {results['summary']['average_accuracy']:.2%}")
print(f"Token Savings: {results['summary']['average_token_savings']:.1%}")
```

## Test Cases

The benchmark suite includes 8 predefined test cases:

1. **Email Extraction**: Extract all email addresses
2. **Data Summarization**: Summarize data in 2 sentences
3. **Filtering - Age**: Find users older than 30
4. **Aggregation - Average Age**: Calculate average age
5. **Multi-field Query**: Find users with specific role and age
6. **Count Operations**: Count users by role
7. **Field Extraction - Roles**: Extract unique roles
8. **Data Structure Understanding**: Describe data structure

## Understanding Results

### Console Output

The benchmark prints a summary to console:

```
Accuracy Benchmark Results
==========================
Average Accuracy: 96.5%
Token Savings: 42.3%
Average Latency Difference: -0.2s (TOON faster)
```

### Report Files

Reports are generated in multiple formats:

1. **JSON** (`benchmark_results.json`): Machine-readable detailed results
2. **Markdown** (`benchmark_results.md`): Human-readable formatted report
3. **CSV** (`benchmark_results.csv`): Spreadsheet-compatible data

### Key Metrics

- **Accuracy**: Semantic similarity between JSON and TOON outputs (0-1, higher is better)
- **Token Savings**: Percentage reduction in tokens (higher is better)
- **Latency Difference**: Time difference (negative = TOON faster)

### Interpreting Accuracy Scores

- **> 0.95**: Excellent - outputs are semantically equivalent
- **0.85 - 0.95**: Good - minor differences but same meaning
- **0.70 - 0.85**: Acceptable - some differences but core information preserved
- **< 0.70**: Poor - significant differences, may indicate information loss

## Adding New Test Cases

Edit `tests/accuracy_benchmarks/test_suite.py`:

```python
from tests.accuracy_benchmarks.test_suite import TestCase, TEST_CASES

# Add new test case
TEST_CASES.append(
    TestCase(
        name="Your Test Name",
        task="Your task description",
        evaluation_method="semantic_similarity",  # or "extraction", "numeric", "exact_match"
        field="emails",  # Optional, for extraction tasks
        ground_truth=None  # Optional
    )
)
```

## Troubleshooting

### "GROQ_API_KEY not found"

Ensure `.env` file exists at project root and contains `GROQ_API_KEY=your_key`.

### "No data returned from query"

Ensure your test database has data. The benchmark uses queries from `fixtures/sample_queries.py`.

### "sentence-transformers not available"

Install it: `pip install sentence-transformers`. The benchmark will fall back to exact matching if not available.

### High latency

Groq API latency depends on model and request size. Try:
- Using a faster model (`llama-3.1-8b-instant`)
- Reducing `BENCHMARK_MAX_TOKENS` in `.env`
- Using smaller queries

## Best Practices

1. **Run multiple times**: Set `BENCHMARK_NUM_RUNS` for statistical significance
2. **Use realistic data**: Test with data similar to your production use case
3. **Compare models**: Test different Groq models to find best accuracy/speed tradeoff
4. **Monitor costs**: Groq API has rate limits and costs
5. **Document results**: Save reports for comparison over time

## Integration with CI/CD

To run benchmarks in CI/CD:

```yaml
# Example GitHub Actions
- name: Run Accuracy Benchmarks
  env:
    GROQ_API_KEY: ${{ secrets.GROQ_API_KEY }}
  run: |
    python -m tests.accuracy_benchmarks.test_accuracy \
      --db-type postgresql \
      --query small_users
```

Note: CI/CD runs may require API keys in secrets and may have rate limits.

## Example Output

See `benchmark_results/` directory after running benchmarks for example reports.

## Contributing

To add new evaluation methods or test cases, see:
- `tests/accuracy_benchmarks/metrics.py` - Add new metric functions
- `tests/accuracy_benchmarks/test_suite.py` - Add new test cases
- `tests/accuracy_benchmarks/fixtures/sample_queries.py` - Add new queries

