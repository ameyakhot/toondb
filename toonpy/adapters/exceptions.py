"""
Custom exceptions for toonpy adapters
"""


class ToonDBError(Exception):
    """Base exception class for all toonpy errors"""
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

