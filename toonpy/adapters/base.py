from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional

class BaseAdapter(ABC):
    """Base class for all database adapters"""
    
    def __init__(self, verbose: bool = False, tokenizer_model: str = "gpt-4", log_file: Optional[str] = None, enable_logging: bool = True):
        """
        Initialize adapter with optional verbose mode for token auditing.
        
        Args:
            verbose: If True, track and display token statistics (default: False)
            tokenizer_model: Model name for tokenizer (default: "gpt-4")
            log_file: Path to log file for token statistics (default: None, uses stdout)
            enable_logging: If False, disable logging even when verbose=True (default: True)
        """
        from toonpy.core.stats import SessionStats
        self.stats = SessionStats(enabled=verbose, tokenizer_model=tokenizer_model)
        self.log_file = log_file
        self.enable_logging = enable_logging and verbose  # Only enable if verbose is True
        
        # Open log file in append mode if specified
        if self.log_file and self.enable_logging:
            try:
                # Create directory if it doesn't exist
                import os
                log_dir = os.path.dirname(self.log_file)
                if log_dir and not os.path.exists(log_dir):
                    os.makedirs(log_dir, exist_ok=True)
            except Exception:
                # If file creation fails, fall back to stdout
                self.log_file = None

    @abstractmethod
    def query(self, query: str) -> str:
        """Execute a query and return the results"""
        pass

    @abstractmethod
    def close(self):
        """Close connection"""
        pass

    def _to_toon(self, results: List[Dict], query_type: str = "query") -> str:
        """
        Convert results to TOON format and track statistics if verbose mode is enabled.
        
        Args:
            results: List of dictionaries (query results)
            query_type: Type of query (e.g., "find", "query", "aggregate") for stats tracking
        
        Returns:
            str: TOON formatted string
        """
        from toonpy.core.converter import to_toon
        import json
        from toonpy.core.token_counter import count_tokens, count_chars, get_tokenizer_name
        
        toon_result = to_toon(results)
        
        if self.stats.enabled:
            # Count JSON representation
            json_str = json.dumps(results, separators=(',', ':'))
            json_chars = count_chars(json_str)
            json_tokens = count_tokens(json_str, self.stats.tokenizer_model)
            
            # Count TOON representation
            toon_chars = count_chars(toon_result)
            toon_tokens = count_tokens(toon_result, self.stats.tokenizer_model)
            
            # Add to stats
            tokenizer_name = get_tokenizer_name(self.stats.tokenizer_model)
            self.stats.add_query(
                json_chars=json_chars,
                json_tokens=json_tokens,
                toon_chars=toon_chars,
                toon_tokens=toon_tokens,
                query_type=query_type,
                tokenizer_name=tokenizer_name
            )
            
            # Write log line to file or stdout
            if self.enable_logging:
                log_line = self.stats.get_formatted_log(self.stats.queries[-1])
                if self.log_file:
                    try:
                        with open(self.log_file, 'a', encoding='utf-8') as f:
                            f.write(log_line + '\n')
                    except Exception:
                        # Fall back to stdout if file write fails
                        print(log_line)
                else:
                    print(log_line)
        
        return toon_result
    
    def get_stats(self, detailed: bool = False) -> Dict[str, Any]:
        """
        Get session statistics.
        
        Args:
            detailed: If True, include per-query stats (default: False)
        
        Returns:
            Dict: Statistics dictionary
        """
        return self.stats.to_dict(detailed=detailed)
    
    def reset_stats(self):
        """Reset session statistics"""
        self.stats.reset()
    
    def enable_stats(self):
        """Enable statistics tracking"""
        self.stats.enabled = True
    
    def disable_stats(self):
        """Disable statistics tracking"""
        self.stats.enabled = False
    
    def print_stats(self):
        """Print formatted statistics summary"""
        summary = self.stats.get_summary()
        if summary["total_queries"] == 0:
            print("No statistics available. Enable verbose mode to track stats.")
            return
        
        print("\n=== TOON Statistics Summary ===")
        print(f"Total Queries: {summary['total_queries']}")
        print(f"Tokenizer: {summary['tokenizer_name']}")
        print(f"\nJSON: {summary['total_json_chars']} chars ({summary['total_json_tokens']} tokens)")
        print(f"TOON: {summary['total_toon_chars']} chars ({summary['total_toon_tokens']} tokens)")
        print(f"\nTotal Saved: {summary['total_chars_saved']} chars ({summary['total_tokens_saved']} tokens)")
        print(f"Average Savings: {summary['average_savings_chars_percent']:.1f}% chars, {summary['average_savings_tokens_percent']:.1f}% tokens")
        print(f"Total Savings: {summary['total_savings_chars_percent']:.1f}% chars, {summary['total_savings_tokens_percent']:.1f}% tokens")
        print(f"\nQueries by Type: {summary['queries_by_type']}")
        print("=" * 32)
    
    def disable_logging(self):
        """Disable logging (stops writing logs even if verbose=True)"""
        self.enable_logging = False
    
    def start_logging(self):
        """Enable logging (only works if verbose=True)"""
        if self.stats.enabled:
            self.enable_logging = True
        else:
            raise ValueError("Cannot enable logging when verbose=False. Set verbose=True first.")
    
    def set_log_file(self, log_file: Optional[str]):
        """
        Set or change the log file path.
        
        Args:
            log_file: Path to log file, or None to use stdout
        """
        self.log_file = log_file
        if log_file:
            try:
                import os
                log_dir = os.path.dirname(log_file)
                if log_dir and not os.path.exists(log_dir):
                    os.makedirs(log_dir, exist_ok=True)
            except Exception:
                self.log_file = None
        

