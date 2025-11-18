"""
TOON Database Adapter Library
Convert your database queries to TOON format for efficient LLM usage.
"""

from toonpy.adapters.mongo_adapter import MongoAdapter
from toonpy.adapters.postgres_adapter import PostgresAdapter
from toonpy.adapters.mysql_adapter import MySQLAdapter
from toonpy.adapters.exceptions import (
    ToonDBError,
    ConnectionError,
    QueryError,
    SchemaError
)
from toonpy.core.converter import to_toon, from_toon

__version__ = "0.2.0"

__all__ = [
    "MongoAdapter",
    "PostgresAdapter",
    "MySQLAdapter",
    "ToonDBError",
    "ConnectionError",
    "QueryError",
    "SchemaError",
    "to_toon",
    "from_toon",
]