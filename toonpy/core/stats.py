"""
Statistics tracking for TOON format token auditing
Tracks JSON vs TOON character and token counts per query and session
"""
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
import time


@dataclass
class QueryStats:
    """Stats for a single query"""
    json_chars: int = 0
    json_tokens: int = 0
    toon_chars: int = 0
    toon_tokens: int = 0
    tokenizer_name: str = ""
    query_type: str = ""  # e.g., "find", "query", "aggregate"
    timestamp: float = field(default_factory=time.time)
    
    @property
    def savings_chars_percent(self) -> float:
        """Calculate percentage savings for characters"""
        if self.json_chars == 0:
            return 0.0
        return ((self.json_chars - self.toon_chars) / self.json_chars) * 100
    
    @property
    def savings_tokens_percent(self) -> float:
        """Calculate percentage savings for tokens"""
        if self.json_tokens == 0:
            return 0.0
        return ((self.json_tokens - self.toon_tokens) / self.json_tokens) * 100
    
    @property
    def chars_saved(self) -> int:
        """Calculate absolute characters saved"""
        return self.json_chars - self.toon_chars
    
    @property
    def tokens_saved(self) -> int:
        """Calculate absolute tokens saved"""
        return self.json_tokens - self.toon_tokens


class SessionStats:
    """Session-level statistics tracking"""
    
    def __init__(self, enabled: bool = False, tokenizer_model: str = "gpt-4"):
        """
        Initialize session statistics.
        
        Args:
            enabled: Whether stats tracking is enabled
            tokenizer_model: Model name for tokenizer (e.g., "gpt-4")
        """
        self.enabled = enabled
        self.tokenizer_model = tokenizer_model
        self.queries: List[QueryStats] = []
    
    def add_query(
        self,
        json_chars: int,
        json_tokens: int,
        toon_chars: int,
        toon_tokens: int,
        query_type: str = "query",
        tokenizer_name: str = ""
    ):
        """
        Record a query's stats.
        
        Args:
            json_chars: Character count for JSON representation
            json_tokens: Token count for JSON representation
            toon_chars: Character count for TOON representation
            toon_tokens: Token count for TOON representation
            query_type: Type of query (e.g., "find", "query", "aggregate")
            tokenizer_name: Human-readable tokenizer name
        """
        if self.enabled:
            self.queries.append(QueryStats(
                json_chars=json_chars,
                json_tokens=json_tokens,
                toon_chars=toon_chars,
                toon_tokens=toon_tokens,
                query_type=query_type,
                tokenizer_name=tokenizer_name
            ))
    
    def get_formatted_log(self, query_stats: QueryStats) -> str:
        """
        Format a single query's stats as a log line.
        
        Args:
            query_stats: QueryStats instance to format
        
        Returns:
            str: Formatted log line
        
        Format:
            [TOON Stats] JSON: 62 chars (18 tokens) | TOON: 21 chars (7 tokens) | Savings: 66.1% chars, 61.1% tokens [gpt-4]
        """
        savings_chars = query_stats.savings_chars_percent
        savings_tokens = query_stats.savings_tokens_percent
        
        return (
            f"[TOON Stats] JSON: {query_stats.json_chars} chars ({query_stats.json_tokens} tokens) | "
            f"TOON: {query_stats.toon_chars} chars ({query_stats.toon_tokens} tokens) | "
            f"Savings: {savings_chars:.1f}% chars, {savings_tokens:.1f}% tokens "
            f"[{query_stats.tokenizer_name}]"
        )
    
    def get_all_logs(self) -> List[str]:
        """
        Get formatted log lines for all queries.
        
        Returns:
            List[str]: List of formatted log lines
        """
        return [self.get_formatted_log(q) for q in self.queries]
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get summary statistics for the session.
        
        Returns:
            Dict: Summary statistics with:
                - total_queries: int
                - total_json_chars: int
                - total_json_tokens: int
                - total_toon_chars: int
                - total_toon_tokens: int
                - total_chars_saved: int
                - total_tokens_saved: int
                - average_savings_chars_percent: float
                - average_savings_tokens_percent: float
                - total_savings_chars_percent: float
                - total_savings_tokens_percent: float
                - queries_by_type: Dict[str, int]
                - tokenizer_name: str
        """
        if not self.queries:
            return {
                "total_queries": 0,
                "total_json_chars": 0,
                "total_json_tokens": 0,
                "total_toon_chars": 0,
                "total_toon_tokens": 0,
                "total_chars_saved": 0,
                "total_tokens_saved": 0,
                "average_savings_chars_percent": 0.0,
                "average_savings_tokens_percent": 0.0,
                "total_savings_chars_percent": 0.0,
                "total_savings_tokens_percent": 0.0,
                "queries_by_type": {},
                "tokenizer_name": self.queries[0].tokenizer_name if self.queries else ""
            }
        
        total_json_chars = sum(q.json_chars for q in self.queries)
        total_json_tokens = sum(q.json_tokens for q in self.queries)
        total_toon_chars = sum(q.toon_chars for q in self.queries)
        total_toon_tokens = sum(q.toon_tokens for q in self.queries)
        total_chars_saved = total_json_chars - total_toon_chars
        total_tokens_saved = total_json_tokens - total_toon_tokens
        
        avg_savings_chars = sum(q.savings_chars_percent for q in self.queries) / len(self.queries)
        avg_savings_tokens = sum(q.savings_tokens_percent for q in self.queries) / len(self.queries)
        
        total_savings_chars_pct = (total_chars_saved / total_json_chars * 100) if total_json_chars > 0 else 0.0
        total_savings_tokens_pct = (total_tokens_saved / total_json_tokens * 100) if total_json_tokens > 0 else 0.0
        
        # Count queries by type
        queries_by_type = {}
        for q in self.queries:
            queries_by_type[q.query_type] = queries_by_type.get(q.query_type, 0) + 1
        
        tokenizer_name = self.queries[0].tokenizer_name if self.queries else ""
        
        return {
            "total_queries": len(self.queries),
            "total_json_chars": total_json_chars,
            "total_json_tokens": total_json_tokens,
            "total_toon_chars": total_toon_chars,
            "total_toon_tokens": total_toon_tokens,
            "total_chars_saved": total_chars_saved,
            "total_tokens_saved": total_tokens_saved,
            "average_savings_chars_percent": avg_savings_chars,
            "average_savings_tokens_percent": avg_savings_tokens,
            "total_savings_chars_percent": total_savings_chars_pct,
            "total_savings_tokens_percent": total_savings_tokens_pct,
            "queries_by_type": queries_by_type,
            "tokenizer_name": tokenizer_name
        }
    
    def reset(self):
        """Reset all statistics"""
        self.queries.clear()
    
    def to_dict(self, detailed: bool = False) -> Dict[str, Any]:
        """
        Convert to dictionary for serialization.
        
        Args:
            detailed: If True, include per-query stats
        
        Returns:
            Dict: Statistics dictionary
        """
        result = {
            "enabled": self.enabled,
            "tokenizer_model": self.tokenizer_model,
            "summary": self.get_summary()
        }
        
        if detailed:
            result["queries"] = [
                {
                    "json_chars": q.json_chars,
                    "json_tokens": q.json_tokens,
                    "toon_chars": q.toon_chars,
                    "toon_tokens": q.toon_tokens,
                    "savings_chars_percent": q.savings_chars_percent,
                    "savings_tokens_percent": q.savings_tokens_percent,
                    "chars_saved": q.chars_saved,
                    "tokens_saved": q.tokens_saved,
                    "query_type": q.query_type,
                    "tokenizer_name": q.tokenizer_name,
                    "timestamp": q.timestamp
                }
                for q in self.queries
            ]
        
        return result

