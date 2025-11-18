from toonpy.adapters.base import BaseAdapter
from toonpy.adapters.exceptions import ConnectionError, QueryError, SchemaError
from typing import Optional, Dict, Any, List
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import connection as PgConnection
from datetime import datetime, date, time
from decimal import Decimal
import uuid
import base64


class PostgresAdapter(BaseAdapter):
    """Adapter for PostgreSQL databases"""
    
    def __init__(
        self,
        connection_string: Optional[str] = None,
        connection: Optional[Any] = None,
        **kwargs
    ):
        """
        Initialize PostgreSQL Adapter

        Args:
            connection_string: PostgreSQL connection string (postgresql://user:pass@host:port/dbname)
            connection: Existing psycopg2 connection object
            **kwargs: Additional connection parameters (host, port, user, password, database)

        Raises:
            ConnectionError: If connection cannot be established
            ValueError: If invalid configuration is provided
        """
        if connection is not None:
            self._validate_connection(connection)
            self.connection = connection
            self.own_connection = False
        elif connection_string:
            try:
                self.connection = psycopg2.connect(connection_string)
                self.own_connection = True
            except psycopg2.OperationalError as e:
                raise ConnectionError(f"Failed to connect to PostgreSQL: {e}") from e
        elif kwargs:
            try:
                self.connection = psycopg2.connect(**kwargs)
                self.own_connection = True
            except psycopg2.OperationalError as e:
                raise ConnectionError(f"Failed to connect to PostgreSQL: {e}") from e
        else:
            raise ValueError(
                "Invalid configuration. Provide either connection, "
                "connection_string, or connection parameters (host, port, user, password, database)."
            )
        
        # Check if connection is closed
        if self.connection.closed:
            raise ConnectionError("Connection is closed")
    
    def _validate_connection(self, conn: Any) -> None:
        """
        Validate that the connection object is a psycopg2 connection

        Args:
            conn: Connection object to validate

        Raises:
            ValueError: If connection is not a valid psycopg2 connection
        """
        if not isinstance(conn, PgConnection):
            raise ValueError(
                f"Connection must be a psycopg2 connection object, got {type(conn)}"
            )
    
    def query(self, sql: str) -> str:
        """
        Execute SQL query and return results in TOON format

        Args:
            sql: SQL query string

        Returns:
            str: TOON formatted string

        Raises:
            ConnectionError: If connection is closed or unavailable
            QueryError: If query execution fails
        """
        if self.connection.closed:
            raise ConnectionError("Connection is closed")
        
        try:
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            cursor.execute(sql)
            
            # Check if query returns rows (SELECT queries)
            if cursor.description:
                # SELECT query - fetch results
                results = cursor.fetchall()
                data = [dict(row) for row in results]
                data = self._clean_postgres_data(data)
                cursor.close()
                return self._to_toon(data)
            else:
                # Non-SELECT query (INSERT, UPDATE, DELETE)
                # Return empty TOON (empty list)
                cursor.close()
                return self._to_toon([])
        
        except psycopg2.OperationalError as e:
            # Rollback on connection error
            try:
                self.connection.rollback()
            except:
                pass
            raise ConnectionError(f"Connection error during query execution: {e}") from e
        except (psycopg2.ProgrammingError, psycopg2.IntegrityError) as e:
            # Rollback on query error to allow subsequent queries
            try:
                self.connection.rollback()
            except:
                pass
            raise QueryError(f"Query execution failed: {e}") from e
        except Exception as e:
            # Rollback on unexpected error
            try:
                self.connection.rollback()
            except:
                pass
            raise QueryError(f"Unexpected error during query execution: {e}") from e
    
    def execute(self, sql: str) -> str:
        """
        Execute SQL query (alias for query method)

        Args:
            sql: SQL query string

        Returns:
            str: TOON formatted string

        Raises:
            ConnectionError: If connection is closed or unavailable
            QueryError: If query execution fails
        """
        return self.query(sql)
    
    def _clean_postgres_data(self, docs: List[Dict]) -> List[Dict]:
        """
        Convert PostgreSQL data types to JSON-serializable format

        Args:
            docs: List of dictionaries from PostgreSQL query results

        Returns:
            List[Dict]: Cleaned data ready for TOON encoding
        """
        cleaned = []
        for doc in docs:
            cleaned_doc = {}
            for key, value in doc.items():
                cleaned_doc[key] = self._clean_value(value)
            cleaned.append(cleaned_doc)
        return cleaned
    
    def _clean_value(self, value: Any) -> Any:
        """
        Clean a single value, handling nested structures recursively

        Args:
            value: Value to clean

        Returns:
            Cleaned value
        """
        if value is None:
            return None
        elif isinstance(value, Decimal):
            return float(value)
        elif isinstance(value, (datetime, date, time)):
            return value.isoformat()
        elif isinstance(value, uuid.UUID):
            return str(value)
        elif isinstance(value, bytes):
            # bytea type - convert to base64 string
            return base64.b64encode(value).decode('utf-8')
        elif isinstance(value, (list, tuple)):
            # PostgreSQL arrays
            return [self._clean_value(item) for item in value]
        elif isinstance(value, dict):
            # JSON/JSONB types (already dict/list)
            return {k: self._clean_value(v) for k, v in value.items()}
        elif isinstance(value, (int, float, str, bool)):
            return value
        else:
            # Fallback for unknown types (custom types, network types, etc.)
            return str(value)
    
    def get_schema(self, table: Optional[str] = None, schema: str = 'public') -> Dict:
        """
        Get database schema information

        Args:
            table: Table name (optional, if None returns all tables in schema)
            schema: Schema name (default: 'public')

        Returns:
            Dict: Schema information with structure:
                {table_name: {columns: [{name, data_type, is_nullable, column_default}]}}

        Raises:
            ConnectionError: If connection is closed or unavailable
            SchemaError: If schema discovery fails
        """
        if self.connection.closed:
            raise ConnectionError("Connection is closed")
        
        try:
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            
            if table:
                # Get single table schema
                query = """
                    SELECT 
                        column_name,
                        data_type,
                        is_nullable,
                        column_default
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position;
                """
                cursor.execute(query, (schema, table))
                columns = [dict(row) for row in cursor.fetchall()]
                cursor.close()
                
                if not columns:
                    raise SchemaError(f"Table '{schema}.{table}' not found")
                
                return {table: {"columns": columns}}
            else:
                # Get all tables in schema
                query = """
                    SELECT DISTINCT table_name
                    FROM information_schema.tables
                    WHERE table_schema = %s
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name;
                """
                cursor.execute(query, (schema,))
                tables = [row['table_name'] for row in cursor.fetchall()]
                cursor.close()
                
                schema_dict = {}
                for table_name in tables:
                    schema_dict[table_name] = self.get_schema(table_name, schema)[table_name]
                
                return schema_dict
        
        except SchemaError:
            raise
        except psycopg2.OperationalError as e:
            raise ConnectionError(f"Connection error during schema discovery: {e}") from e
        except psycopg2.ProgrammingError as e:
            raise SchemaError(f"Schema discovery failed: {e}") from e
        except Exception as e:
            raise SchemaError(f"Unexpected error during schema discovery: {e}") from e
    
    def get_tables(self, include_views: bool = False, schema: str = 'public') -> List[str]:
        """
        List all tables in the database

        Args:
            include_views: If True, include views and materialized views (default: False)
            schema: Schema name (default: 'public')

        Returns:
            List[str]: List of table names

        Raises:
            ConnectionError: If connection is closed or unavailable
            SchemaError: If table listing fails
        """
        if self.connection.closed:
            raise ConnectionError("Connection is closed")
        
        try:
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            
            if include_views:
                query = """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = %s
                    AND table_type IN ('BASE TABLE', 'VIEW', 'MATERIALIZED VIEW')
                    ORDER BY table_name;
                """
            else:
                query = """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = %s
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name;
                """
            
            cursor.execute(query, (schema,))
            tables = [row['table_name'] for row in cursor.fetchall()]
            cursor.close()
            return tables
        
        except psycopg2.OperationalError as e:
            raise ConnectionError(f"Connection error during table listing: {e}") from e
        except psycopg2.ProgrammingError as e:
            raise SchemaError(f"Table listing failed: {e}") from e
        except Exception as e:
            raise SchemaError(f"Unexpected error during table listing: {e}") from e
    
    def close(self):
        """
        Close PostgreSQL connection if adapter owns it

        Raises:
            ConnectionError: If connection cannot be closed
        """
        if self.own_connection and self.connection and not self.connection.closed:
            try:
                self.connection.close()
            except Exception as e:
                raise ConnectionError(f"Error closing connection: {e}") from e

