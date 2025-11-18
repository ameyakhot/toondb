"""
Custom exceptions for toondb adapters
"""


class ToonDBError(Exception):
    """Base exception class for all toondb errors"""
    pass


class ConnectionError(ToonDBError):
    """Raised when there are connection-related errors"""
    pass


class QueryError(ToonDBError):
    """Raised when there are SQL query execution errors"""
    pass


class SchemaError(ToonDBError):
    """Raised when there are schema discovery errors"""
    pass

