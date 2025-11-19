"""
Report generator for accuracy benchmark results.
Generates console, JSON, Markdown, and CSV reports.
"""
import json
import csv
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime


class ReportGenerator:
    """Generate reports from benchmark results"""
    
    def __init__(self, results: Dict[str, Any], output_dir: Path):
        """
        Initialize report generator.
        
        Args:
            results: Benchmark results dictionary
            output_dir: Directory to save reports
        """
        self.results = results
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_all(self):
        """Generate all report formats"""
        self.generate_console()
        self.generate_json()
        self.generate_markdown()
        self.generate_csv()
    
    def generate_console(self):
        """Print console summary"""
        summary = self.results['summary']
        config = self.results['config']
        
        print("\n" + "=" * 60)
        print("Accuracy Benchmark Results")
        print("=" * 60)
        print(f"\nConfiguration:")
        print(f"  Database: {config['db_type']}")
        print(f"  Query: {config['query_name']}")
        print(f"  Model: {config['model']}")
        print(f"  Test Cases: {config['num_test_cases']}")
        
        print(f"\nSummary:")
        print(f"  Average Accuracy: {summary['average_accuracy']:.2%}")
        print(f"  Accuracy Range: {summary['min_accuracy']:.2%} - {summary['max_accuracy']:.2%}")
        print(f"  Average Token Savings: {summary['average_token_savings']:.1%}")
        print(f"  Total Tokens Saved: {summary['total_token_savings']:.0f}")
        print(f"  Average Latency Difference: {summary['average_latency_diff']:.3f}s")
        
        print(f"\nTask-by-Task Results:")
        for result in self.results['results']:
            print(f"  {result['test_name']}:")
            print(f"    Accuracy: {result['accuracy']:.2%}")
            print(f"    Token Savings: {result['token_savings']:.1%}")
            print(f"    Latency: JSON={result['json_latency']:.3f}s, TOON={result['toon_latency']:.3f}s")
        
        print("=" * 60)
    
    def generate_json(self):
        """Generate JSON report"""
        output_file = self.output_dir / "benchmark_results.json"
        
        # Add metadata
        report_data = {
            'timestamp': datetime.now().isoformat(),
            'config': self.results['config'],
            'summary': self.results['summary'],
            'results': self.results['results']
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)
        
        print(f"\nJSON report saved to: {output_file}")
    
    def generate_markdown(self):
        """Generate Markdown report"""
        output_file = self.output_dir / "benchmark_results.md"
        
        summary = self.results['summary']
        config = self.results['config']
        
        md_content = f"""# Accuracy Benchmark Results

**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Configuration

- **Database:** {config['db_type']}
- **Query:** {config['query_name']}
- **LLM Model:** {config['model']}
- **Test Cases:** {config['num_test_cases']}

## Summary

| Metric | Value |
|--------|-------|
| Average Accuracy | {summary['average_accuracy']:.2%} |
| Min Accuracy | {summary['min_accuracy']:.2%} |
| Max Accuracy | {summary['max_accuracy']:.2%} |
| Average Token Savings | {summary['average_token_savings']:.1%} |
| Total Tokens Saved | {summary['total_token_savings']:.0f} |
| Average Latency Difference | {summary['average_latency_diff']:.3f}s |

## Task-by-Task Results

"""
        
        for result in self.results['results']:
            md_content += f"""### {result['test_name']}

**Task:** {result['task']}

**Metrics:**
- Accuracy: {result['accuracy']:.2%}
- Token Savings: {result['token_savings']:.1%}
- JSON Tokens: {result['json_tokens']}
- TOON Tokens: {result['toon_tokens']}
- JSON Latency: {result['json_latency']:.3f}s
- TOON Latency: {result['toon_latency']:.3f}s
- Latency Difference: {result['latency_diff']:.3f}s

**JSON Response:**
```
{result['json_response'][:500]}{'...' if len(result['json_response']) > 500 else ''}
```

**TOON Response:**
```
{result['toon_response'][:500]}{'...' if len(result['toon_response']) > 500 else ''}
```

---
"""
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(md_content)
        
        print(f"Markdown report saved to: {output_file}")
    
    def generate_csv(self):
        """Generate CSV report"""
        output_file = self.output_dir / "benchmark_results.csv"
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Header
            writer.writerow([
                'Test Name',
                'Task',
                'Evaluation Method',
                'Accuracy',
                'JSON Tokens',
                'TOON Tokens',
                'Token Savings',
                'JSON Latency (s)',
                'TOON Latency (s)',
                'Latency Difference (s)'
            ])
            
            # Data rows
            for result in self.results['results']:
                writer.writerow([
                    result['test_name'],
                    result['task'],
                    result['evaluation_method'],
                    f"{result['accuracy']:.4f}",
                    result['json_tokens'],
                    result['toon_tokens'],
                    f"{result['token_savings']:.4f}",
                    f"{result['json_latency']:.4f}",
                    f"{result['toon_latency']:.4f}",
                    f"{result['latency_diff']:.4f}"
                ])
        
        print(f"CSV report saved to: {output_file}")

