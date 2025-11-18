from toonpy.adapters.base import BaseAdapter
from toonpy.adapters.exceptions import ConnectionError, QueryError, SchemaError, SecurityError
from typing import Optional, Dict, Any, List, Union, Tuple
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
    
    def query(self, sql: str, params: Optional[Union[Tuple, Dict, List]] = None) -> str:
        """
        Execute SQL query and return results in TOON format.
        
        For security, use parameterized queries when including user input:
        - Safe: query("SELECT * FROM users WHERE id = %s", (123,))
        - Safe: query("SELECT * FROM users WHERE name = %s AND age > %s", ("Alice", 30))
        - Unsafe: query("SELECT * FROM users WHERE id = " + str(user_id))  # SQL injection risk

        Args:
            sql: SQL query string (use %s placeholders for parameters)
            params: Optional parameters for parameterized query (tuple, dict, or list).
                   When provided, prevents SQL injection by using database parameterization.
                   When None, executes raw SQL (use with caution).

        Returns:
            str: TOON formatted string

        Raises:
            ConnectionError: If connection is closed or unavailable
            QueryError: If query execution fails
            SecurityError: If security validation fails (when validate=True)
        
        Examples:
            >>> # Safe parameterized query (recommended)
            >>> adapter.query("SELECT * FROM users WHERE id = %s", (123,))
            >>> adapter.query("SELECT * FROM users WHERE name = %s AND age > %s", ("Alice", 30))
            >>> 
            >>> # Raw SQL (use with caution - no user input)
            >>> adapter.query("SELECT * FROM users WHERE id = 123")
        """
        if not self.connection.open:
            raise ConnectionError("Connection is closed")
        
        try:
            cursor = self.connection.cursor(DictCursor)
            
            # Use parameterized query if params provided (prevents SQL injection)
            if params is not None:
                cursor.execute(sql, params)
            else:
                # Raw SQL execution (user responsibility to prevent injection)
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
    
    def execute(self, sql: str, params: Optional[Union[Tuple, Dict, List]] = None) -> str:
        """
        Execute SQL query (alias for query method)

        Args:
            sql: SQL query string (use %s placeholders for parameters)
            params: Optional parameters for parameterized query (tuple, dict, or list)

        Returns:
            str: TOON formatted string

        Raises:
            ConnectionError: If connection is closed or unavailable
            QueryError: If query execution fails
        """
        return self.query(sql, params)
    
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
    
    def _validate_table_name(self, table: str) -> None:
        """
        Validate table name to prevent SQL injection.
        
        Args:
            table: Table name to validate
        
        Raises:
            SecurityError: If table name contains invalid characters
        """
        # Allow alphanumeric, underscore, and dot (for database.table)
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', table):
            raise SecurityError(f"Invalid table name: {table}. Only alphanumeric, underscore, and dot allowed.")
    
    def _validate_column_names(self, table: str, columns: List[str], database: Optional[str] = None) -> None:
        """
        Validate that columns exist in the table schema.
        
        Args:
            table: Table name
            columns: List of column names to validate
            database: Database name (optional, defaults to current database)
        
        Raises:
            SchemaError: If any column doesn't exist
        
        Note:
            If schema lookup fails, validation is skipped (graceful degradation)
        """
        try:
            table_schema = self.get_schema(table, database)
            valid_columns = {col['column_name'] for col in table_schema[table]['columns']}
            
            for col in columns:
                if col not in valid_columns:
                    raise SchemaError(f"Column '{col}' does not exist in table '{database or 'current'}.{table}'")
        except SchemaError:
            # Re-raise SchemaError (column doesn't exist or table doesn't exist)
            raise
        except Exception:
            # If schema lookup fails for other reasons, skip validation
            # This allows operations to proceed even if schema discovery fails
            pass
    
    def _convert_to_mysql_value(self, value: Any, column_type: Optional[str] = None) -> Any:
        """
        Convert Python value to MySQL-compatible type.
        
        Args:
            value: Value from TOON (after from_toon())
            column_type: Optional MySQL column type from schema
        
        Returns:
            MySQL-compatible value
        """
        if value is None:
            return None
        
        # Handle date/time strings (from TOON)
        if isinstance(value, str):
            # Try to parse ISO date/time strings
            try:
                # Try datetime first
                if 'T' in value or len(value) > 10:
                    return datetime.fromisoformat(value.replace('Z', '+00:00'))
                # Try date
                return date.fromisoformat(value)
            except (ValueError, AttributeError):
                pass
            
            # Try UUID string (MySQL has no native UUID type, use CHAR(36))
            if column_type and ('char' in column_type.lower() or 'varchar' in column_type.lower()):
                # Check if it looks like a UUID
                uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                if re.match(uuid_pattern, value, re.IGNORECASE):
                    # Return as string (will be stored as CHAR(36) or VARCHAR(36))
                    return value
            
            # Try base64 for BLOB
            if column_type and ('blob' in column_type.lower() or 'binary' in column_type.lower()):
                try:
                    return base64.b64decode(value)
                except Exception:
                    pass
        
        # Handle lists (for JSON)
        if isinstance(value, list):
            return [self._convert_to_mysql_value(item, column_type) for item in value]
        
        # Handle dicts (for JSON)
        if isinstance(value, dict):
            return {k: self._convert_to_mysql_value(v, column_type) for k, v in value.items()}
        
        # Return as-is for primitives
        return value
    
    def _generate_insert_sql(
        self, 
        table: str, 
        data: Union[Dict, List[Dict]], 
        database: Optional[str] = None,
        on_duplicate_key_update: Optional[str] = None
    ) -> Tuple[str, List[Any]]:
        """
        Generate parameterized INSERT SQL.
        
        Args:
            table: Table name
            data: Single dict or list of dicts with row data
            database: Database name (optional, defaults to current database)
            on_duplicate_key_update: Optional ON DUPLICATE KEY UPDATE clause
        
        Returns:
            Tuple of (SQL string, parameter list)
        
        Raises:
            SecurityError: If table name is invalid
            SchemaError: If columns don't exist
        """
        self._validate_table_name(table)
        
        # Handle single row or multiple rows
        if isinstance(data, dict):
            rows = [data]
        else:
            rows = data
        
        if len(rows) == 0:
            raise ValueError("No data provided for INSERT")
        
        # Get column names from first row
        columns = list(rows[0].keys())
        
        # Validate columns exist
        self._validate_column_names(table, columns, database)
        
        # Build SQL
        if database:
            table_qualified = f"{database}.{table}"
        else:
            table_qualified = table
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        
        # Build VALUES clause for all rows
        if len(rows) == 1:
            values_clause = f"VALUES ({placeholders})"
            params = [rows[0][col] for col in columns]
        else:
            # Multi-row INSERT
            values_list = []
            params = []
            for row in rows:
                values_list.append(f"({placeholders})")
                params.extend([row.get(col) for col in columns])
            values_clause = f"VALUES {', '.join(values_list)}"
        
        sql = f"INSERT INTO {table_qualified} ({columns_str}) {values_clause}"
        
        if on_duplicate_key_update:
            sql += f" ON DUPLICATE KEY UPDATE {on_duplicate_key_update}"
        
        return sql, params
    
    def _generate_update_sql(
        self,
        table: str,
        data: Dict,
        where: Dict[str, Any],
        database: Optional[str] = None
    ) -> Tuple[str, List[Any]]:
        """
        Generate parameterized UPDATE SQL.
        
        Args:
            table: Table name
            data: Dict with column values to update
            where: Dict with WHERE clause conditions
            database: Database name (optional, defaults to current database)
        
        Returns:
            Tuple of (SQL string, parameter list)
        
        Raises:
            SecurityError: If table name is invalid
            SchemaError: If columns don't exist
        """
        self._validate_table_name(table)
        
        if not data:
            raise ValueError("No update data provided")
        
        if not where:
            raise ValueError("WHERE clause is required for UPDATE")
        
        # Validate columns
        update_columns = list(data.keys())
        where_columns = list(where.keys())
        self._validate_column_names(table, update_columns + where_columns, database)
        
        # Build SET clause
        set_clauses = [f"{col} = %s" for col in update_columns]
        set_sql = ", ".join(set_clauses)
        
        # Build WHERE clause
        where_clauses = [f"{col} = %s" for col in where_columns]
        where_sql = " AND ".join(where_clauses)
        
        # Build parameters
        params = [data[col] for col in update_columns] + [where[col] for col in where_columns]
        
        if database:
            table_qualified = f"{database}.{table}"
        else:
            table_qualified = table
        sql = f"UPDATE {table_qualified} SET {set_sql} WHERE {where_sql}"
        
        return sql, params
    
    def _generate_delete_sql(
        self,
        table: str,
        where: Dict[str, Any],
        database: Optional[str] = None
    ) -> Tuple[str, List[Any]]:
        """
        Generate parameterized DELETE SQL.
        
        Args:
            table: Table name
            where: Dict with WHERE clause conditions
            database: Database name (optional, defaults to current database)
        
        Returns:
            Tuple of (SQL string, parameter list)
        
        Raises:
            SecurityError: If table name is invalid
            SchemaError: If columns don't exist
        """
        self._validate_table_name(table)
        
        if not where:
            raise ValueError("WHERE clause is required for DELETE")
        
        # Validate columns
        where_columns = list(where.keys())
        self._validate_column_names(table, where_columns, database)
        
        # Build WHERE clause
        where_clauses = [f"{col} = %s" for col in where_columns]
        where_sql = " AND ".join(where_clauses)
        
        # Build parameters
        params = [where[col] for col in where_columns]
        
        if database:
            table_qualified = f"{database}.{table}"
        else:
            table_qualified = table
        sql = f"DELETE FROM {table_qualified} WHERE {where_sql}"
        
        return sql, params
    
    def insert_one_from_toon(
        self, 
        table: str, 
        toon_string: str, 
        database: Optional[str] = None,
        on_duplicate_key_update: Optional[str] = None
    ) -> str:
        """
        Insert single row from TOON format.
        
        Flow: TOON → from_toon() → Type conversion → Generate INSERT SQL → Execute → Return TOON
        
        Args:
            table: Table name
            toon_string: TOON formatted string with row data
            database: Database name (optional, defaults to current database)
            on_duplicate_key_update: MySQL ON DUPLICATE KEY UPDATE clause (e.g., "name = VALUES(name)")
        
        Returns:
            str: TOON formatted string with insert result (rowcount)
        
        Raises:
            ConnectionError: If connection is closed
            QueryError: If insert fails
            SchemaError: If table/columns don't exist
            SecurityError: If table name is invalid
        """
        from toonpy.core.converter import from_toon
        
        if not self.connection.open:
            raise ConnectionError("Connection is closed")
        
        try:
            # Convert TOON to Python data
            data = from_toon(toon_string)
            
            # Handle both single dict and list with one dict
            if isinstance(data, list):
                if len(data) == 0:
                    raise ValueError("TOON string must contain at least one row")
                row = data[0]
            elif isinstance(data, dict):
                row = data
            else:
                raise ValueError(f"TOON string must decode to a dict or list of dicts, got {type(data)}")
            
            # Get schema for type conversion
            try:
                table_schema = self.get_schema(table, database)
                column_types = {col['column_name']: col['data_type'] for col in table_schema[table]['columns']}
            except SchemaError:
                column_types = {}
            
            # Convert values to MySQL types
            converted_row = {}
            for key, value in row.items():
                col_type = column_types.get(key)
                converted_row[key] = self._convert_to_mysql_value(value, col_type)
            
            # Generate SQL
            sql, params = self._generate_insert_sql(table, converted_row, database, on_duplicate_key_update)
            
            # Execute
            cursor = self.connection.cursor()
            cursor.execute(sql, params)
            rowcount = cursor.rowcount
            cursor.close()
            self.connection.commit()
            
            # Return result as TOON
            result_dict = {"rowcount": rowcount}
            return self._to_toon([result_dict])
        
        except (SecurityError, SchemaError, ValueError):
            raise
        except pymysql.OperationalError as e:
            try:
                self.connection.rollback()
            except:
                pass
            raise ConnectionError(f"Connection error during insert: {e}") from e
        except (pymysql.ProgrammingError, pymysql.IntegrityError) as e:
            try:
                self.connection.rollback()
            except:
                pass
            raise QueryError(f"Insert failed: {e}") from e
        except Exception as e:
            try:
                self.connection.rollback()
            except:
                pass
            raise QueryError(f"Unexpected error during insert: {e}") from e
    
    def insert_many_from_toon(
        self, 
        table: str, 
        toon_string: str, 
        database: Optional[str] = None,
        on_duplicate_key_update: Optional[str] = None
    ) -> str:
        """
        Insert multiple rows from TOON format using bulk INSERT.
        
        Flow: TOON → from_toon() → Type conversion → Generate bulk INSERT SQL → Execute → Return TOON
        
        Args:
            table: Table name
            toon_string: TOON formatted string with list of rows
            database: Database name (optional, defaults to current database)
            on_duplicate_key_update: MySQL ON DUPLICATE KEY UPDATE clause
        
        Returns:
            str: TOON formatted string with insert result (rowcount)
        
        Raises:
            ConnectionError: If connection is closed
            QueryError: If insert fails
            SchemaError: If table/columns don't exist
            SecurityError: If table name is invalid
        """
        from toonpy.core.converter import from_toon
        
        if not self.connection.open:
            raise ConnectionError("Connection is closed")
        
        try:
            # Convert TOON to Python data
            data = from_toon(toon_string)
            
            # Ensure data is a list
            if not isinstance(data, list):
                data = [data]
            
            if len(data) == 0:
                raise ValueError("TOON string must contain at least one row")
            
            # Get schema for type conversion
            try:
                table_schema = self.get_schema(table, database)
                column_types = {col['column_name']: col['data_type'] for col in table_schema[table]['columns']}
            except SchemaError:
                column_types = {}
            
            # Convert values to MySQL types
            converted_rows = []
            for row in data:
                converted_row = {}
                for key, value in row.items():
                    col_type = column_types.get(key)
                    converted_row[key] = self._convert_to_mysql_value(value, col_type)
                converted_rows.append(converted_row)
            
            # Generate SQL
            sql, params = self._generate_insert_sql(table, converted_rows, database, on_duplicate_key_update)
            
            # Execute
            cursor = self.connection.cursor()
            cursor.execute(sql, params)
            rowcount = cursor.rowcount
            cursor.close()
            self.connection.commit()
            
            # Return result as TOON
            result_dict = {"rowcount": rowcount}
            return self._to_toon([result_dict])
        
        except (SecurityError, SchemaError, ValueError):
            raise
        except pymysql.OperationalError as e:
            try:
                self.connection.rollback()
            except:
                pass
            raise ConnectionError(f"Connection error during insert: {e}") from e
        except (pymysql.ProgrammingError, pymysql.IntegrityError) as e:
            try:
                self.connection.rollback()
            except:
                pass
            raise QueryError(f"Insert failed: {e}") from e
        except Exception as e:
            try:
                self.connection.rollback()
            except:
                pass
            raise QueryError(f"Unexpected error during insert: {e}") from e
    
    def update_from_toon(
        self,
        table: str,
        toon_string: str,
        where: Dict[str, Any],
        database: Optional[str] = None
    ) -> str:
        """
        Update rows from TOON format.
        
        Flow: TOON → from_toon() → Type conversion → Generate UPDATE SQL → Execute → Return TOON
        
        Args:
            table: Table name
            toon_string: TOON formatted string with update data
            where: WHERE clause conditions as dict (e.g., {"id": 123, "status": "active"})
            database: Database name (optional, defaults to current database)
        
        Returns:
            str: TOON formatted string with update result (rowcount)
        
        Raises:
            ConnectionError: If connection is closed
            QueryError: If update fails
            SchemaError: If table/columns don't exist
            SecurityError: If table name is invalid
        """
        from toonpy.core.converter import from_toon
        
        if not self.connection.open:
            raise ConnectionError("Connection is closed")
        
        try:
            # Convert TOON to Python data
            update_data = from_toon(toon_string)
            
            # Handle both single dict and list with one dict
            if isinstance(update_data, list):
                if len(update_data) == 0:
                    raise ValueError("TOON string must contain update data")
                data = update_data[0]
            elif isinstance(update_data, dict):
                data = update_data
            else:
                raise ValueError(f"TOON string must decode to a dict or list of dicts, got {type(update_data)}")
            
            # Get schema for type conversion
            try:
                table_schema = self.get_schema(table, database)
                column_types = {col['column_name']: col['data_type'] for col in table_schema[table]['columns']}
            except SchemaError:
                column_types = {}
            
            # Convert values to MySQL types
            converted_data = {}
            for key, value in data.items():
                col_type = column_types.get(key)
                converted_data[key] = self._convert_to_mysql_value(value, col_type)
            
            # Convert WHERE values
            converted_where = {}
            for key, value in where.items():
                col_type = column_types.get(key)
                converted_where[key] = self._convert_to_mysql_value(value, col_type)
            
            # Generate SQL
            sql, params = self._generate_update_sql(table, converted_data, converted_where, database)
            
            # Execute
            cursor = self.connection.cursor()
            cursor.execute(sql, params)
            rowcount = cursor.rowcount
            cursor.close()
            self.connection.commit()
            
            # Return result as TOON
            result_dict = {"rowcount": rowcount}
            return self._to_toon([result_dict])
        
        except (SecurityError, SchemaError, ValueError):
            raise
        except pymysql.OperationalError as e:
            try:
                self.connection.rollback()
            except:
                pass
            raise ConnectionError(f"Connection error during update: {e}") from e
        except (pymysql.ProgrammingError, pymysql.IntegrityError) as e:
            try:
                self.connection.rollback()
            except:
                pass
            raise QueryError(f"Update failed: {e}") from e
        except Exception as e:
            try:
                self.connection.rollback()
            except:
                pass
            raise QueryError(f"Unexpected error during update: {e}") from e
    
    def delete_from_toon(
        self,
        table: str,
        where: Dict[str, Any],
        database: Optional[str] = None
    ) -> str:
        """
        Delete rows based on WHERE conditions.
        
        Flow: Generate DELETE SQL from WHERE dict → Execute with params → Return result as TOON
        
        Args:
            table: Table name
            where: WHERE clause conditions as dict
            database: Database name (optional, defaults to current database)
        
        Returns:
            str: TOON formatted string with delete result (rowcount)
        
        Raises:
            ConnectionError: If connection is closed
            QueryError: If delete fails
            SchemaError: If table/columns don't exist
            SecurityError: If table name is invalid
        """
        if not self.connection.open:
            raise ConnectionError("Connection is closed")
        
        try:
            # Get schema for type conversion
            try:
                table_schema = self.get_schema(table, database)
                column_types = {col['column_name']: col['data_type'] for col in table_schema[table]['columns']}
            except SchemaError:
                column_types = {}
            
            # Convert WHERE values
            converted_where = {}
            for key, value in where.items():
                col_type = column_types.get(key)
                converted_where[key] = self._convert_to_mysql_value(value, col_type)
            
            # Generate SQL
            sql, params = self._generate_delete_sql(table, converted_where, database)
            
            # Execute
            cursor = self.connection.cursor()
            cursor.execute(sql, params)
            rowcount = cursor.rowcount
            cursor.close()
            self.connection.commit()
            
            # Return result as TOON
            result_dict = {"rowcount": rowcount}
            return self._to_toon([result_dict])
        
        except (SecurityError, SchemaError, ValueError):
            raise
        except pymysql.OperationalError as e:
            try:
                self.connection.rollback()
            except:
                pass
            raise ConnectionError(f"Connection error during delete: {e}") from e
        except (pymysql.ProgrammingError, pymysql.IntegrityError) as e:
            try:
                self.connection.rollback()
            except:
                pass
            raise QueryError(f"Delete failed: {e}") from e
        except Exception as e:
            try:
                self.connection.rollback()
            except:
                pass
            raise QueryError(f"Unexpected error during delete: {e}") from e
    
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

