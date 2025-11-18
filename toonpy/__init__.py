"""
TOON Database Adapter Library
Convert your database queries to TOON format for efficient LLM usage.
"""

from typing import Optional, Union
from toonpy.adapters.mongo_adapter import MongoAdapter
from toonpy.adapters.postgres_adapter import PostgresAdapter
from toonpy.adapters.mysql_adapter import MySQLAdapter
from toonpy.adapters.base import BaseAdapter
from toonpy.adapters.exceptions import (
    ToonDBError,
    ConnectionError,
    QueryError,
    SchemaError
)
from toonpy.core.converter import to_toon, from_toon

__version__ = "0.2.0"

__all__ = [
    "connect",
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


def _handle_unrecognized_connection_string(connection_string: str) -> None:
    """
    Handle unrecognized connection strings with helpful error messages.
    
    Args:
        connection_string: The unrecognized connection string
    
    Raises:
        ValueError: With helpful error message and suggestions
    """
    connection_string_lower = connection_string.lower().strip()
    
    # Common mistakes and suggestions
    suggestions = []
    
    # Check for common variations
    if "sqlite" in connection_string_lower:
        suggestions.append(
            "SQLite is not currently supported. "
            "Supported databases: PostgreSQL, MySQL, MongoDB"
        )
    elif "oracle" in connection_string_lower:
        suggestions.append(
            "Oracle is not currently supported. "
            "Supported databases: PostgreSQL, MySQL, MongoDB"
        )
    elif "mssql" in connection_string_lower or "sqlserver" in connection_string_lower:
        suggestions.append(
            "SQL Server is not currently supported. "
            "Supported databases: PostgreSQL, MySQL, MongoDB"
        )
    elif "redis" in connection_string_lower:
        suggestions.append(
            "Redis is not currently supported. "
            "Supported databases: PostgreSQL, MySQL, MongoDB"
        )
    elif "://" not in connection_string:
        suggestions.append(
            "Connection string appears to be missing a protocol prefix. "
            "Try: postgresql://, mysql://, or mongodb://"
        )
    elif connection_string_lower.startswith("http://") or connection_string_lower.startswith("https://"):
        suggestions.append(
            "HTTP/HTTPS URLs are not database connection strings. "
            "Use database-specific protocols: postgresql://, mysql://, or mongodb://"
        )
    elif connection_string_lower.startswith("jdbc:"):
        suggestions.append(
            "JDBC connection strings are not supported. "
            "Use native connection strings: postgresql://, mysql://, or mongodb://"
        )
    else:
        # Generic suggestion
        suggestions.append(
            "Connection string format not recognized. "
            "Ensure it starts with one of: postgresql://, mysql://, or mongodb://"
        )
    
    # Build error message
    error_msg = (
        f"Unrecognized connection string format: {connection_string}\n\n"
        f"Supported connection string formats:\n"
        f"  - PostgreSQL: postgresql://user:pass@host:port/dbname\n"
        f"  - MySQL: mysql://user:pass@host:port/dbname\n"
        f"  - MongoDB: mongodb://host:port (requires database and collection_name parameters)\n\n"
    )
    
    if suggestions:
        error_msg += "Suggestion: " + suggestions[0] + "\n\n"
    
    error_msg += (
        "Alternative: Specify db_type explicitly:\n"
        "  connect(connection_string='...', db_type='postgresql')\n"
        "  connect(connection_string='...', db_type='mysql')\n"
        "  connect(connection_string='...', db_type='mongodb', database='...', collection_name='...')"
    )
    
    raise ValueError(error_msg)


def connect(
    connection_string: Optional[str] = None,
    db_type: Optional[str] = None,
    **kwargs
) -> BaseAdapter:
    """
    Unified connection function for all database types.
    
    Auto-detects database type from connection string or accepts explicit type.
    
    Args:
        connection_string: Database connection string
            - PostgreSQL: postgresql://user:pass@host:port/dbname or postgres://...
            - MySQL: mysql://user:pass@host:port/dbname or mysql+pymysql://...
            - MongoDB: mongodb://host:port or mongodb+srv://... (requires database and collection_name)
        db_type: Explicit database type ("postgresql", "mysql", "mongodb")
        **kwargs: Additional connection parameters
            - For PostgreSQL/MySQL: host, port, user, password, database
            - For MongoDB: database, collection_name (required with connection_string), or collection
    
    Returns:
        BaseAdapter: Appropriate adapter instance (PostgresAdapter, MySQLAdapter, or MongoAdapter)
    
    Raises:
        ValueError: If connection string format is unrecognized or configuration is invalid
        ConnectionError: If connection cannot be established
    
    Examples:
        >>> # PostgreSQL - auto-detected
        >>> adapter = connect("postgresql://user:pass@localhost:5432/mydb")
        >>> result = adapter.query("SELECT * FROM users")
        >>> 
        >>> # MySQL - auto-detected
        >>> adapter = connect("mysql://user:pass@localhost:3306/mydb")
        >>> result = adapter.query("SELECT * FROM products")
        >>> 
        >>> # MongoDB - requires database and collection_name
        >>> adapter = connect("mongodb://localhost:27017", database="mydb", collection_name="users")
        >>> result = adapter.find({"age": {"$gt": 30}})
        >>> 
        >>> # Explicit type specification
        >>> adapter = connect("postgres://...", db_type="postgresql")
        >>> 
        >>> # Individual parameters
        >>> adapter = connect(
        ...     db_type="postgresql",
        ...     host="localhost",
        ...     port=5432,
        ...     user="user",
        ...     password="pass",
        ...     database="mydb"
        ... )
    """
    detected_type = None
    
    # Auto-detect from connection string if type not specified
    if not db_type and connection_string:
        connection_string_lower = connection_string.lower().strip()
        
        # PostgreSQL variations
        if connection_string_lower.startswith(("postgresql://", "postgres://")):
            detected_type = "postgresql"
        # MySQL variations
        elif connection_string_lower.startswith(("mysql://", "mysql+pymysql://")):
            detected_type = "mysql"
        # MongoDB variations
        elif connection_string_lower.startswith(("mongodb://", "mongodb+srv://")):
            detected_type = "mongodb"
        else:
            # Unrecognized connection string - provide helpful error
            _handle_unrecognized_connection_string(connection_string)
    
    # Use detected type or explicit type
    db_type = db_type or detected_type
    
    if not db_type:
        raise ValueError(
            "Could not determine database type. "
            "Please provide either:\n"
            "  1. A connection string with a recognized prefix:\n"
            "     - postgresql:// or postgres:// (PostgreSQL)\n"
            "     - mysql:// or mysql+pymysql:// (MySQL)\n"
            "     - mongodb:// or mongodb+srv:// (MongoDB)\n"
            "  2. An explicit db_type parameter: 'postgresql', 'mysql', or 'mongodb'\n"
            "  3. Individual connection parameters with db_type specified"
        )
    
    db_type = db_type.lower()
    
    # Route to appropriate adapter
    if db_type in ("postgresql", "postgres"):
        return PostgresAdapter(connection_string=connection_string, **kwargs)
    elif db_type == "mysql":
        return MySQLAdapter(connection_string=connection_string, **kwargs)
    elif db_type == "mongodb":
        # MongoDB requires database and collection_name when using connection_string
        if connection_string:
            if "collection" not in kwargs:
                if "database" not in kwargs or "collection_name" not in kwargs:
                    raise ValueError(
                        "MongoDB connection requires 'database' and 'collection_name' parameters "
                        "when using connection_string. Alternatively, provide a 'collection' object.\n\n"
                        "Example: connect('mongodb://localhost:27017', db_type='mongodb', "
                        "database='mydb', collection_name='users')"
                    )
        return MongoAdapter(
            connection_string=connection_string,
            database=kwargs.get("database"),
            collection_name=kwargs.get("collection_name"),
            collection=kwargs.get("collection")
        )
    else:
        raise ValueError(
            f"Unsupported database type: {db_type}. "
            f"Supported types: 'postgresql', 'mysql', 'mongodb'"
        )