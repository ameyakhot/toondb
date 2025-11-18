from toonpy.adapters.base import BaseAdapter
from toonpy.adapters.exceptions import ConnectionError, QueryError, SchemaError
from typing import Optional, Dict, Any, List
import pymysql
from pymysql.cursors import DictCursor
from pymysql.connections import Connection as MySQLConnection
from datetime import datetime, date, time
from decimal import Decimal
import base64
import re


class MySQLAdapter(BaseAdapter):
    """Adapter for MySQL databases"""
    
    def __init__(
        self,
        connection_string: Optional[str] = None,
        connection: Optional[Any] = None,
        **kwargs
    ):
        """
        Initialize MySQL Adapter

        Args:
            connection_string: MySQL connection string (mysql://user:pass@host:port/dbname)
            connection: Existing pymysql connection object
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
                # Parse connection string and connect
                conn_params = self._parse_connection_string(connection_string)
                self.connection = pymysql.connect(**conn_params)
                self.own_connection = True
            except pymysql.OperationalError as e:
                raise ConnectionError(f"Failed to connect to MySQL: {e}") from e
            except Exception as e:
                raise ConnectionError(f"Failed to connect to MySQL: {e}") from e
        elif kwargs:
            try:
                self.connection = pymysql.connect(**kwargs)
                self.own_connection = True
            except pymysql.OperationalError as e:
                raise ConnectionError(f"Failed to connect to MySQL: {e}") from e
        else:
            raise ValueError(
                "Invalid configuration. Provide either connection, "
                "connection_string, or connection parameters (host, port, user, password, database)."
            )
        
        # Check if connection is open
        if not self.connection.open:
            raise ConnectionError("Connection is closed")
    
    def _validate_connection(self, conn: Any) -> None:
        """
        Validate that the connection object is a pymysql connection

        Args:
            conn: Connection object to validate

        Raises:
            ValueError: If connection is not a valid pymysql connection
        """
        if not isinstance(conn, MySQLConnection):
            raise ValueError(
                f"Connection must be a pymysql connection object, got {type(conn)}"
            )
    
    def _parse_connection_string(self, connection_string: str) -> Dict[str, Any]:
        """
        Parse MySQL connection string (mysql://user:pass@host:port/dbname)

        Args:
            connection_string: MySQL connection string

        Returns:
            Dict with connection parameters

        Raises:
            ValueError: If connection string format is invalid
        """
        # Remove mysql:// prefix
        if connection_string.startswith('mysql://'):
            connection_string = connection_string[8:]
        elif connection_string.startswith('mysql+pymysql://'):
            connection_string = connection_string[16:]
        else:
            raise ValueError("Connection string must start with mysql:// or mysql+pymysql://")
        
        # Parse user:pass@host:port/dbname
        pattern = r'^(?:([^:]+):([^@]+)@)?([^:/]+)(?::(\d+))?(?:/(.+))?$'
        match = re.match(pattern, connection_string)
        
        if not match:
            raise ValueError(f"Invalid connection string format: {connection_string}")
        
        user, password, host, port, database = match.groups()
        
        params = {}
        if host:
            params['host'] = host
        if port:
            params['port'] = int(port)
        else:
            params['port'] = 3306  # Default MySQL port
        if user:
            params['user'] = user
        if password:
            params['password'] = password
        if database:
            params['database'] = database
        
        return params
    
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
        if not self.connection.open:
            raise ConnectionError("Connection is closed")
        
        try:
            cursor = self.connection.cursor(DictCursor)
            cursor.execute(sql)
            
            # Check if query returns rows (SELECT queries)
            if cursor.description:
                # SELECT query - fetch results
                results = cursor.fetchall()
                data = [dict(row) for row in results]
                data = self._clean_mysql_data(data)
                cursor.close()
                return self._to_toon(data)
            else:
                # Non-SELECT query (INSERT, UPDATE, DELETE)
                # Commit transaction explicitly for DML operations
                self.connection.commit()
                cursor.close()
                return self._to_toon([])
        
        except pymysql.OperationalError as e:
            # Rollback on connection error
            try:
                self.connection.rollback()
            except:
                pass
            raise ConnectionError(f"Connection error during query execution: {e}") from e
        except (pymysql.ProgrammingError, pymysql.IntegrityError) as e:
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
    
    def _clean_mysql_data(self, docs: List[Dict]) -> List[Dict]:
        """
        Convert MySQL data types to JSON-serializable format

        Args:
            docs: List of dictionaries from MySQL query results

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
        elif isinstance(value, bytes):
            # BLOB types - convert to base64 string
            return base64.b64encode(value).decode('utf-8')
        elif isinstance(value, (list, tuple)):
            # SET types or arrays
            return [self._clean_value(item) for item in value]
        elif isinstance(value, dict):
            # JSON types (already dict/list)
            return {k: self._clean_value(v) for k, v in value.items()}
        elif isinstance(value, (int, float, str, bool)):
            return value
        else:
            # Fallback for unknown types (ENUM, custom types, etc.)
            return str(value)
    
    def get_schema(self, table: Optional[str] = None, database: Optional[str] = None) -> Dict:
        """
        Get database schema information

        Args:
            table: Table name (optional, if None returns all tables in database)
            database: Database name (optional, defaults to current database)

        Returns:
            Dict: Schema information with structure:
                {table_name: {columns: [{name, data_type, is_nullable, column_default}]}}

        Raises:
            ConnectionError: If connection is closed or unavailable
            SchemaError: If schema discovery fails
        """
        if not self.connection.open:
            raise ConnectionError("Connection is closed")
        
        try:
            cursor = self.connection.cursor(DictCursor)
            
            # Get current database if not specified
            if database is None:
                cursor.execute("SELECT DATABASE() as db")
                result = cursor.fetchone()
                database = result['db'] if result and result['db'] else None
            
            if not database:
                raise SchemaError("No database specified and no current database")
            
            if table:
                # Get single table schema
                query = """
                    SELECT 
                        COLUMN_NAME as column_name,
                        DATA_TYPE as data_type,
                        IS_NULLABLE as is_nullable,
                        COLUMN_DEFAULT as column_default
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                    ORDER BY ORDINAL_POSITION;
                """
                cursor.execute(query, (database, table))
                columns = [dict(row) for row in cursor.fetchall()]
                cursor.close()
                
                if not columns:
                    raise SchemaError(f"Table '{database}.{table}' not found")
                
                return {table: {"columns": columns}}
            else:
                # Get all tables in database
                query = """
                    SELECT DISTINCT TABLE_NAME as table_name
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = %s
                    AND TABLE_TYPE = 'BASE TABLE'
                    ORDER BY TABLE_NAME;
                """
                cursor.execute(query, (database,))
                tables = [row['table_name'] for row in cursor.fetchall()]
                cursor.close()
                
                schema_dict = {}
                for table_name in tables:
                    schema_dict[table_name] = self.get_schema(table_name, database)[table_name]
                
                return schema_dict
        
        except SchemaError:
            raise
        except pymysql.OperationalError as e:
            raise ConnectionError(f"Connection error during schema discovery: {e}") from e
        except pymysql.ProgrammingError as e:
            raise SchemaError(f"Schema discovery failed: {e}") from e
        except Exception as e:
            raise SchemaError(f"Unexpected error during schema discovery: {e}") from e
    
    def get_tables(self, include_views: bool = False, database: Optional[str] = None) -> List[str]:
        """
        List all tables in the database

        Args:
            include_views: If True, include views (default: False)
            database: Database name (optional, defaults to current database)

        Returns:
            List[str]: List of table names

        Raises:
            ConnectionError: If connection is closed or unavailable
            SchemaError: If table listing fails
        """
        if not self.connection.open:
            raise ConnectionError("Connection is closed")
        
        try:
            cursor = self.connection.cursor(DictCursor)
            
            # Get current database if not specified
            if database is None:
                cursor.execute("SELECT DATABASE() as db")
                result = cursor.fetchone()
                database = result['db'] if result and result['db'] else None
            
            if not database:
                raise SchemaError("No database specified and no current database")
            
            if include_views:
                query = """
                    SELECT TABLE_NAME as table_name
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = %s
                    AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')
                    ORDER BY TABLE_NAME;
                """
            else:
                query = """
                    SELECT TABLE_NAME as table_name
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = %s
                    AND TABLE_TYPE = 'BASE TABLE'
                    ORDER BY TABLE_NAME;
                """
            
            cursor.execute(query, (database,))
            tables = [row['table_name'] for row in cursor.fetchall()]
            cursor.close()
            return tables
        
        except pymysql.OperationalError as e:
            raise ConnectionError(f"Connection error during table listing: {e}") from e
        except pymysql.ProgrammingError as e:
            raise SchemaError(f"Table listing failed: {e}") from e
        except Exception as e:
            raise SchemaError(f"Unexpected error during table listing: {e}") from e
    
    def close(self):
        """
        Close MySQL connection if adapter owns it

        Raises:
            ConnectionError: If connection cannot be closed
        """
        if self.own_connection and self.connection and self.connection.open:
            try:
                self.connection.close()
            except Exception as e:
                raise ConnectionError(f"Error closing connection: {e}") from e

