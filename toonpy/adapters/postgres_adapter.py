from toonpy.adapters.base import BaseAdapter
from toonpy.adapters.exceptions import ConnectionError, QueryError, SchemaError, SecurityError
from typing import Optional, Dict, Any, List, Union, Tuple
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
        verbose: bool = False,
        tokenizer_model: str = "gpt-4",
        log_file: Optional[str] = None,
        enable_logging: bool = True,
        **kwargs
    ):
        """
        Initialize PostgreSQL Adapter

        Args:
            connection_string: PostgreSQL connection string (postgresql://user:pass@host:port/dbname)
            connection: Existing psycopg2 connection object
            verbose: If True, track and display token statistics (default: False)
            tokenizer_model: Model name for tokenizer (default: "gpt-4")
            log_file: Path to log file for token statistics (default: None, uses stdout)
            enable_logging: If False, disable logging even when verbose=True (default: True)
            **kwargs: Additional connection parameters (host, port, user, password, database)

        Raises:
            ConnectionError: If connection cannot be established
            ValueError: If invalid configuration is provided
        """
        super().__init__(verbose=verbose, tokenizer_model=tokenizer_model, log_file=log_file, enable_logging=enable_logging)
        
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
        if self.connection.closed:
            raise ConnectionError("Connection is closed")
        
        try:
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            
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
                data = self._clean_postgres_data(data)
                cursor.close()
                return self._to_toon(data, query_type="query")
            else:
                # Non-SELECT query (INSERT, UPDATE, DELETE)
                # Return empty TOON (empty list)
                cursor.close()
                return self._to_toon([], query_type="query")
        
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
    
    def _validate_table_name(self, table: str) -> None:
        """
        Validate table name to prevent SQL injection.
        
        Args:
            table: Table name to validate
        
        Raises:
            SecurityError: If table name contains invalid characters
        """
        # Allow alphanumeric, underscore, and dot (for schema.table)
        import re
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', table):
            raise SecurityError(f"Invalid table name: {table}. Only alphanumeric, underscore, and dot allowed.")
    
    def _validate_column_names(self, table: str, columns: List[str], schema: str = 'public') -> None:
        """
        Validate that columns exist in the table schema.
        
        Args:
            table: Table name
            columns: List of column names to validate
            schema: Schema name
        
        Raises:
            SchemaError: If any column doesn't exist
        
        Note:
            If schema lookup fails, validation is skipped (graceful degradation)
        """
        try:
            table_schema = self.get_schema(table, schema)
            valid_columns = {col['column_name'] for col in table_schema[table]['columns']}
            
            for col in columns:
                if col not in valid_columns:
                    raise SchemaError(f"Column '{col}' does not exist in table '{schema}.{table}'")
        except SchemaError:
            # Re-raise SchemaError (column doesn't exist or table doesn't exist)
            raise
        except Exception:
            # If schema lookup fails for other reasons, skip validation
            # This allows operations to proceed even if schema discovery fails
            pass
    
    def _generate_insert_sql(
        self, 
        table: str, 
        data: Union[Dict, List[Dict]], 
        schema: str = 'public',
        on_conflict: Optional[str] = None
    ) -> Tuple[str, List[Any]]:
        """
        Generate parameterized INSERT SQL.
        
        Args:
            table: Table name
            data: Single dict or list of dicts with row data
            schema: Schema name
            on_conflict: Optional ON CONFLICT clause
        
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
        self._validate_column_names(table, columns, schema)
        
        # Build SQL
        table_qualified = f"{schema}.{table}" if schema != 'public' else table
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
        
        if on_conflict:
            sql += f" ON CONFLICT {on_conflict}"
        
        return sql, params
    
    def _generate_update_sql(
        self,
        table: str,
        data: Dict,
        where: Dict[str, Any],
        schema: str = 'public'
    ) -> Tuple[str, List[Any]]:
        """
        Generate parameterized UPDATE SQL.
        
        Args:
            table: Table name
            data: Dict with column values to update
            where: Dict with WHERE clause conditions
            schema: Schema name
        
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
        self._validate_column_names(table, update_columns + where_columns, schema)
        
        # Build SET clause
        set_clauses = [f"{col} = %s" for col in update_columns]
        set_sql = ", ".join(set_clauses)
        
        # Build WHERE clause
        where_clauses = [f"{col} = %s" for col in where_columns]
        where_sql = " AND ".join(where_clauses)
        
        # Build parameters
        params = [data[col] for col in update_columns] + [where[col] for col in where_columns]
        
        table_qualified = f"{schema}.{table}" if schema != 'public' else table
        sql = f"UPDATE {table_qualified} SET {set_sql} WHERE {where_sql}"
        
        return sql, params
    
    def _generate_delete_sql(
        self,
        table: str,
        where: Dict[str, Any],
        schema: str = 'public'
    ) -> Tuple[str, List[Any]]:
        """
        Generate parameterized DELETE SQL.
        
        Args:
            table: Table name
            where: Dict with WHERE clause conditions
            schema: Schema name
        
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
        self._validate_column_names(table, where_columns, schema)
        
        # Build WHERE clause
        where_clauses = [f"{col} = %s" for col in where_columns]
        where_sql = " AND ".join(where_clauses)
        
        # Build parameters
        params = [where[col] for col in where_columns]
        
        table_qualified = f"{schema}.{table}" if schema != 'public' else table
        sql = f"DELETE FROM {table_qualified} WHERE {where_sql}"
        
        return sql, params
    
    def _convert_to_postgres_value(self, value: Any, column_type: Optional[str] = None) -> Any:
        """
        Convert Python value to PostgreSQL-compatible type.
        
        Args:
            value: Value from TOON (after from_toon())
            column_type: Optional PostgreSQL column type from schema
        
        Returns:
            PostgreSQL-compatible value
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
            
            # Try UUID string
            if column_type and 'uuid' in column_type.lower():
                try:
                    return uuid.UUID(value)
                except (ValueError, AttributeError):
                    pass
            
            # Try base64 for bytea
            if column_type and 'bytea' in column_type.lower():
                try:
                    return base64.b64decode(value)
                except Exception:
                    pass
        
        # Handle lists (for arrays)
        if isinstance(value, list):
            return [self._convert_to_postgres_value(item, column_type) for item in value]
        
        # Handle dicts (for JSON/JSONB)
        if isinstance(value, dict):
            return {k: self._convert_to_postgres_value(v, column_type) for k, v in value.items()}
        
        # Return as-is for primitives
        return value
    
    def insert_one_from_toon(
        self, 
        table: str, 
        toon_string: str, 
        schema: str = 'public',
        on_conflict: Optional[str] = None
    ) -> str:
        """
        Insert single row from TOON format.
        
        Flow: TOON → from_toon() → Generate INSERT SQL → Execute with params → Return result as TOON
        
        Args:
            table: Table name
            toon_string: TOON formatted string with row data
            schema: Schema name (default: 'public')
            on_conflict: PostgreSQL ON CONFLICT clause (e.g., "DO NOTHING", "DO UPDATE SET ...")
        
        Returns:
            str: TOON formatted string with insert result (rowcount)
        
        Raises:
            ConnectionError: If connection is closed
            QueryError: If insert fails
            SchemaError: If table/columns don't exist
            SecurityError: If table name is invalid
        """
        from toonpy.core.converter import from_toon
        
        if self.connection.closed:
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
                table_schema = self.get_schema(table, schema)
                column_types = {col['column_name']: col['data_type'] for col in table_schema[table]['columns']}
            except SchemaError:
                column_types = {}
            
            # Convert values to PostgreSQL types
            converted_row = {}
            for key, value in row.items():
                col_type = column_types.get(key)
                converted_row[key] = self._convert_to_postgres_value(value, col_type)
            
            # Generate SQL
            sql, params = self._generate_insert_sql(table, converted_row, schema, on_conflict)
            
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
        except psycopg2.OperationalError as e:
            try:
                self.connection.rollback()
            except:
                pass
            raise ConnectionError(f"Connection error during insert: {e}") from e
        except (psycopg2.ProgrammingError, psycopg2.IntegrityError) as e:
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
        schema: str = 'public',
        on_conflict: Optional[str] = None
    ) -> str:
        """
        Insert multiple rows from TOON format using bulk INSERT.
        
        Flow: TOON → from_toon() → Generate bulk INSERT SQL → Execute with params → Return result as TOON
        
        Args:
            table: Table name
            toon_string: TOON formatted string with list of rows
            schema: Schema name (default: 'public')
            on_conflict: PostgreSQL ON CONFLICT clause
        
        Returns:
            str: TOON formatted string with insert result (rowcount)
        
        Raises:
            ConnectionError: If connection is closed
            QueryError: If insert fails
            SchemaError: If table/columns don't exist
            SecurityError: If table name is invalid
        """
        from toonpy.core.converter import from_toon
        
        if self.connection.closed:
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
                table_schema = self.get_schema(table, schema)
                column_types = {col['column_name']: col['data_type'] for col in table_schema[table]['columns']}
            except SchemaError:
                column_types = {}
            
            # Convert values to PostgreSQL types
            converted_rows = []
            for row in data:
                converted_row = {}
                for key, value in row.items():
                    col_type = column_types.get(key)
                    converted_row[key] = self._convert_to_postgres_value(value, col_type)
                converted_rows.append(converted_row)
            
            # Generate SQL
            sql, params = self._generate_insert_sql(table, converted_rows, schema, on_conflict)
            
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
        except psycopg2.OperationalError as e:
            try:
                self.connection.rollback()
            except:
                pass
            raise ConnectionError(f"Connection error during insert: {e}") from e
        except (psycopg2.ProgrammingError, psycopg2.IntegrityError) as e:
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
        schema: str = 'public'
    ) -> str:
        """
        Update rows from TOON format.
        
        Flow: TOON → from_toon() → Generate UPDATE SQL → Execute with params → Return result as TOON
        
        Args:
            table: Table name
            toon_string: TOON formatted string with update data
            where: WHERE clause conditions as dict (e.g., {"id": 123, "status": "active"})
            schema: Schema name (default: 'public')
        
        Returns:
            str: TOON formatted string with update result (rowcount)
        
        Raises:
            ConnectionError: If connection is closed
            QueryError: If update fails
            SchemaError: If table/columns don't exist
            SecurityError: If table name is invalid
        """
        from toonpy.core.converter import from_toon
        
        if self.connection.closed:
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
                table_schema = self.get_schema(table, schema)
                column_types = {col['column_name']: col['data_type'] for col in table_schema[table]['columns']}
            except SchemaError:
                column_types = {}
            
            # Convert values to PostgreSQL types
            converted_data = {}
            for key, value in data.items():
                col_type = column_types.get(key)
                converted_data[key] = self._convert_to_postgres_value(value, col_type)
            
            # Convert WHERE values
            converted_where = {}
            for key, value in where.items():
                col_type = column_types.get(key)
                converted_where[key] = self._convert_to_postgres_value(value, col_type)
            
            # Generate SQL
            sql, params = self._generate_update_sql(table, converted_data, converted_where, schema)
            
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
        except psycopg2.OperationalError as e:
            try:
                self.connection.rollback()
            except:
                pass
            raise ConnectionError(f"Connection error during update: {e}") from e
        except (psycopg2.ProgrammingError, psycopg2.IntegrityError) as e:
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
        schema: str = 'public'
    ) -> str:
        """
        Delete rows based on WHERE conditions.
        
        Flow: Generate DELETE SQL from WHERE dict → Execute with params → Return result as TOON
        
        Args:
            table: Table name
            where: WHERE clause conditions as dict
            schema: Schema name (default: 'public')
        
        Returns:
            str: TOON formatted string with delete result (rowcount)
        
        Raises:
            ConnectionError: If connection is closed
            QueryError: If delete fails
            SchemaError: If table/columns don't exist
            SecurityError: If table name is invalid
        """
        if self.connection.closed:
            raise ConnectionError("Connection is closed")
        
        try:
            # Get schema for type conversion
            try:
                table_schema = self.get_schema(table, schema)
                column_types = {col['column_name']: col['data_type'] for col in table_schema[table]['columns']}
            except SchemaError:
                column_types = {}
            
            # Convert WHERE values
            converted_where = {}
            for key, value in where.items():
                col_type = column_types.get(key)
                converted_where[key] = self._convert_to_postgres_value(value, col_type)
            
            # Generate SQL
            sql, params = self._generate_delete_sql(table, converted_where, schema)
            
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
        except psycopg2.OperationalError as e:
            try:
                self.connection.rollback()
            except:
                pass
            raise ConnectionError(f"Connection error during delete: {e}") from e
        except (psycopg2.ProgrammingError, psycopg2.IntegrityError) as e:
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
    
    def insert_and_query_from_toon(
        self,
        table: str,
        toon_string: str,
        where: Optional[Dict[str, Any]] = None,
        schema: str = 'public',
        projection: Optional[List[str]] = None,
        on_conflict: Optional[str] = None
    ) -> str:
        """
        Insert single row from TOON and immediately query it back as TOON.
        Uses the same instance/session - guaranteed to work.
        
        Flow: TOON → insert with RETURNING → query back → TOON (all in same instance)
        
        Args:
            table: Table name
            toon_string: TOON formatted string containing row data
            where: Optional WHERE clause dict to query back inserted row.
                   If None, uses RETURNING clause to get inserted ID or all inserted values
            schema: Schema name (default: 'public')
            projection: Optional list of column names to select (defaults to all columns)
            on_conflict: PostgreSQL ON CONFLICT clause
        
        Returns:
            str: TOON formatted string with queried row data
        
        Example:
            >>> adapter = PostgresAdapter(...)
            >>> toon_data = to_toon([{"name": "Alice", "age": 30}])
            >>> result = adapter.insert_and_query_from_toon("users", toon_data)
            >>> # Returns TOON with the inserted row (with id if exists)
        """
        from toonpy.core.converter import from_toon
        
        if self.connection.closed:
            raise ConnectionError("Connection is closed")
        
        try:
            # Parse inserted data
            data = from_toon(toon_string)
            if isinstance(data, list):
                if len(data) == 0:
                    raise ValueError("TOON string must contain at least one row")
                inserted_row = data[0]
            elif isinstance(data, dict):
                inserted_row = data
            else:
                raise ValueError(f"TOON string must decode to a dict or list of dicts, got {type(data)}")
            
            # Get schema for type conversion
            try:
                table_schema = self.get_schema(table, schema)
                column_types = {col['column_name']: col['data_type'] for col in table_schema[table]['columns']}
            except SchemaError:
                column_types = {}
            
            # Convert values to PostgreSQL types
            converted_row = {}
            for key, value in inserted_row.items():
                col_type = column_types.get(key)
                converted_row[key] = self._convert_to_postgres_value(value, col_type)
            
            # Try to use RETURNING clause for primary key
            if where is None:
                try:
                    table_schema = self.get_schema(table, schema)
                    primary_key_col = None
                    for col in table_schema[table]['columns']:
                        if col.get('is_primary_key') or (col.get('constraint_type') == 'PRIMARY KEY'):
                            primary_key_col = col['column_name']
                            break
                    
                    if primary_key_col:
                        # Use INSERT ... RETURNING
                        sql, params = self._generate_insert_sql(table, converted_row, schema, on_conflict)
                        sql += f" RETURNING {primary_key_col}"
                        
                        cursor = self.connection.cursor()
                        cursor.execute(sql, params)
                        returned_id = cursor.fetchone()[0]
                        cursor.close()
                        self.connection.commit()
                        
                        # Query back by returned ID
                        if projection:
                            cols = ", ".join(projection)
                        else:
                            cols = "*"
                        
                        table_qualified = f"{schema}.{table}" if schema != 'public' else table
                        query_sql = f"SELECT {cols} FROM {table_qualified} WHERE {primary_key_col} = %s"
                        return self.query(query_sql, (returned_id,))
                except (SchemaError, Exception):
                    pass
            
            # Fallback: Insert normally, then query back
            self.insert_one_from_toon(table, toon_string, schema, on_conflict)
            
            # Build query back WHERE clause
            if where is not None:
                query_where = where
            else:
                # Build WHERE from all inserted column values
                query_where = inserted_row
            
            # Build SELECT query with WHERE clause
            table_qualified = f"{schema}.{table}" if schema != 'public' else table
            
            if projection:
                cols = ", ".join(projection)
            else:
                cols = "*"
            
            # Build WHERE clause
            where_clauses = [f"{col} = %s" for col in query_where.keys()]
            where_sql = " AND ".join(where_clauses)
            params = list(query_where.values())
            
            sql = f"SELECT {cols} FROM {table_qualified} WHERE {where_sql} LIMIT 1"
            return self.query(sql, params)
        
        except (ConnectionError, QueryError, SchemaError, SecurityError, ValueError):
            raise
        except Exception as e:
            try:
                self.connection.rollback()
            except:
                pass
            raise QueryError(f"Unexpected error during insert_and_query: {e}") from e
    
    def insert_many_and_query_from_toon(
        self,
        table: str,
        toon_string: str,
        where: Optional[Dict[str, Any]] = None,
        schema: str = 'public',
        projection: Optional[List[str]] = None,
        limit: Optional[int] = None
    ) -> str:
        """
        Insert multiple rows from TOON and immediately query them back as TOON.
        Uses the same instance/session - guaranteed to work.
        
        Flow: TOON → bulk insert → query back → TOON (all in same instance)
        
        Args:
            table: Table name
            toon_string: TOON formatted string containing list of rows
            where: Optional WHERE clause dict to query back inserted rows.
                   If None, queries by all inserted column values using IN operator
            schema: Schema name (default: 'public')
            projection: Optional list of column names to select (defaults to all columns)
            limit: Optional limit on number of rows to return
        
        Returns:
            str: TOON formatted string with queried rows
        
        Example:
            >>> adapter = PostgresAdapter(...)
            >>> toon_data = to_toon([{"name": "Alice"}, {"name": "Bob"}])
            >>> result = adapter.insert_many_and_query_from_toon("users", toon_data)
            >>> # Returns TOON with both inserted rows
        """
        from toonpy.core.converter import from_toon
        
        if self.connection.closed:
            raise ConnectionError("Connection is closed")
        
        try:
            # Insert using existing method
            self.insert_many_from_toon(table, toon_string, schema)
            
            # Parse inserted data
            data = from_toon(toon_string)
            if not isinstance(data, list):
                data = [data]
            
            if len(data) == 0:
                raise ValueError("TOON string must contain at least one row")
            
            # Build query back WHERE clause
            if where is not None:
                query_where = where
                where_clauses = [f"{col} = %s" for col in query_where.keys()]
                where_sql = " AND ".join(where_clauses)
                params = list(query_where.values())
            else:
                # Build WHERE from first row's columns with IN operator for multiple values
                if not data:
                    return self._to_toon([])
                
                first_row = data[0]
                where_parts = []
                params = []
                
                for col in first_row.keys():
                    values = [row.get(col) for row in data if col in row]
                    if len(values) == 1:
                        where_parts.append(f"{col} = %s")
                        params.append(values[0])
                    elif len(values) > 1:
                        placeholders = ", ".join(["%s"] * len(values))
                        where_parts.append(f"{col} IN ({placeholders})")
                        params.extend(values)
                
                where_sql = " AND ".join(where_parts) if where_parts else "1=1"
            
            # Build SELECT query
            table_qualified = f"{schema}.{table}" if schema != 'public' else table
            
            if projection:
                cols = ", ".join(projection)
            else:
                cols = "*"
            
            sql = f"SELECT {cols} FROM {table_qualified} WHERE {where_sql}"
            if limit is not None:
                sql += f" LIMIT {limit}"
            
            return self.query(sql, params)
        
        except (ConnectionError, QueryError, SchemaError, SecurityError, ValueError):
            raise
        except Exception as e:
            try:
                self.connection.rollback()
            except:
                pass
            raise QueryError(f"Unexpected error during insert_many_and_query: {e}") from e
    
    def update_and_query_from_toon(
        self,
        table: str,
        toon_string: str,
        where: Dict[str, Any],
        schema: str = 'public',
        projection: Optional[List[str]] = None
    ) -> str:
        """
        Update rows from TOON and immediately query them back as TOON.
        Uses the same instance/session - guaranteed to work.
        
        Flow: TOON → update → query back using same WHERE → TOON (all in same instance)
        
        Args:
            table: Table name
            toon_string: TOON formatted string with update data
            where: WHERE clause conditions as dict to find rows to update
            schema: Schema name (default: 'public')
            projection: Optional list of column names to select (defaults to all columns)
        
        Returns:
            str: TOON formatted string with updated row data
        
        Example:
            >>> adapter = PostgresAdapter(...)
            >>> update_data = to_toon([{"age": 31, "status": "active"}])
            >>> result = adapter.update_and_query_from_toon(
            ...     "users", 
            ...     update_data,
            ...     where={"id": 123}
            ... )
            >>> # Returns TOON with updated row
        """
        if self.connection.closed:
            raise ConnectionError("Connection is closed")
        
        try:
            # Update using existing method
            self.update_from_toon(table, toon_string, where, schema)
            
            # Build SELECT query with same WHERE clause
            table_qualified = f"{schema}.{table}" if schema != 'public' else table
            
            if projection:
                cols = ", ".join(projection)
            else:
                cols = "*"
            
            # Build WHERE clause (same as update)
            where_clauses = [f"{col} = %s" for col in where.keys()]
            where_sql = " AND ".join(where_clauses)
            params = list(where.values())
            
            sql = f"SELECT {cols} FROM {table_qualified} WHERE {where_sql}"
            return self.query(sql, params)
        
        except (ConnectionError, QueryError, SchemaError, SecurityError, ValueError):
            raise
        except Exception as e:
            try:
                self.connection.rollback()
            except:
                pass
            raise QueryError(f"Unexpected error during update_and_query: {e}") from e
    
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

