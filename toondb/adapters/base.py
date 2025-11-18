from abc import ABC, abstractmethod
from typing import List, Dict, Any

class BaseAdapter(ABC):
    """Base class for all database adapters"""

    @abstractmethod
    def query(self, query: str) -> str:
        """Execute a query and return the results"""
        pass

    @abstractmethod
    def close(self):
        """Close connection"""
        pass

    def _to_toon(self, results: List[Dict]) -> str:
        """Convert results to TOON"""
        from toondb.core.converter import to_toon
        return to_toon(results)
        

