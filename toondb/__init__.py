"""
TOON Database Adapter Library
Convert your database queries to TOON format for efficient LLM usage.
"""

from toondb.adapters.mongo_adapter import MongoAdapter
from toondb.adapters.postgres_adapter import PostgresAdapter
from toondb.adapters.exceptions import (
    ToonDBError,
    ConnectionError,
    QueryError,
    SchemaError
)
from toondb.core.converter import to_toon, from_toon

__version__ = "0.1.0"

__all__ = [
    "MongoAdapter",
    "PostgresAdapter",
    "ToonDBError",
    "ConnectionError",
    "QueryError",
    "SchemaError",
    "to_toon",
    "from_toon",
]