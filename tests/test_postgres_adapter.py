"""
Tests for PostgresAdapter
Includes both unit tests (with mocks) and integration tests (with Docker PostgreSQL)
"""
import unittest
from unittest.mock import Mock, patch, MagicMock
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, date, time
from decimal import Decimal
import uuid

from toonpy import PostgresAdapter, ConnectionError, QueryError, SchemaError

# Connection string for Docker PostgreSQL instance
POSTGRES_CONN_STRING = "postgresql://testuser:testpass@localhost:5433/testdb"


class TestPostgresAdapterUnit(unittest.TestCase):
    """Unit tests with mocked connections"""
    
    @patch('psycopg2.connect')
    def test_init_with_connection_string(self, mock_connect):
        """Test initialization with connection string"""
        mock_conn = Mock()
        mock_conn.closed = False
        mock_connect.return_value = mock_conn
        
        adapter = PostgresAdapter(connection_string=POSTGRES_CONN_STRING)
        self.assertEqual(adapter.connection, mock_conn)
        self.assertTrue(adapter.own_connection)
        adapter.close()
    
    @patch('psycopg2.connect')
    def test_init_with_connection_params(self, mock_connect):
        """Test initialization with individual parameters"""
        mock_conn = Mock()
        mock_conn.closed = False
        mock_connect.return_value = mock_conn
        
        adapter = PostgresAdapter(
            host="localhost",
            port=5432,
            user="testuser",
            password="testpass",
            database="testdb"
        )
        self.assertEqual(adapter.connection, mock_conn)
        self.assertTrue(adapter.own_connection)
        adapter.close()
    
    def test_init_with_existing_connection(self):
        """Test initialization with existing connection object"""
        mock_conn = Mock()
        mock_conn.closed = False
        mock_conn.__class__ = psycopg2.extensions.connection
        
        adapter = PostgresAdapter(connection=mock_conn)
        self.assertEqual(adapter.connection, mock_conn)
        self.assertFalse(adapter.own_connection)
    
    def test_init_with_invalid_connection(self):
        """Test initialization with invalid connection object"""
        invalid_conn = "not a connection"
        
        with self.assertRaises(ValueError):
            PostgresAdapter(connection=invalid_conn)
    
    @patch('psycopg2.connect')
    def test_init_connection_error(self, mock_connect):
        """Test initialization with connection error"""
        mock_connect.side_effect = psycopg2.OperationalError("Connection failed")
        
        with self.assertRaises(ConnectionError):
            PostgresAdapter(connection_string=POSTGRES_CONN_STRING)
    
    @patch('psycopg2.connect')
    def test_query_select(self, mock_connect):
        """Test SELECT query execution"""
        mock_conn = Mock()
        mock_conn.closed = False
        mock_cursor = Mock()
        mock_cursor.description = [('name',), ('age',)]  # Has description = SELECT query
        mock_cursor.fetchall.return_value = [
            {'name': 'Alice', 'age': 30},
            {'name': 'Bob', 'age': 25}
        ]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        adapter = PostgresAdapter(connection_string=POSTGRES_CONN_STRING)
        result = adapter.query("SELECT name, age FROM users")
        
        self.assertIsInstance(result, str)
        self.assertIn("alice", result.lower())
        mock_cursor.execute.assert_called_once()
        adapter.close()
    
    @patch('psycopg2.connect')
    def test_query_non_select(self, mock_connect):
        """Test non-SELECT query execution"""
        mock_conn = Mock()
        mock_conn.closed = False
        mock_cursor = Mock()
        mock_cursor.description = None  # No description = non-SELECT
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        adapter = PostgresAdapter(connection_string=POSTGRES_CONN_STRING)
        result = adapter.query("INSERT INTO users (name) VALUES ('Test')")
        
        self.assertIsInstance(result, str)
        mock_cursor.execute.assert_called_once()
        adapter.close()
    
    @patch('psycopg2.connect')
    def test_query_error(self, mock_connect):
        """Test query error handling"""
        mock_conn = Mock()
        mock_conn.closed = False
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = psycopg2.ProgrammingError("Syntax error")
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        adapter = PostgresAdapter(connection_string=POSTGRES_CONN_STRING)
        with self.assertRaises(QueryError):
            adapter.query("INVALID SQL")
        adapter.close()
    
    @patch('psycopg2.connect')
    def test_query_with_params_tuple(self, mock_connect):
        """Test parameterized query with tuple parameters"""
        mock_conn = Mock()
        mock_conn.closed = False
        mock_cursor = Mock()
        mock_cursor.description = [('name',), ('age',)]
        mock_cursor.fetchall.return_value = [
            {'name': 'Alice', 'age': 30}
        ]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        adapter = PostgresAdapter(connection_string=POSTGRES_CONN_STRING)
        result = adapter.query("SELECT name, age FROM users WHERE id = %s", (123,))
        
        self.assertIsInstance(result, str)
        self.assertIn("alice", result.lower())
        # Verify parameterized query was called
        mock_cursor.execute.assert_called_once_with("SELECT name, age FROM users WHERE id = %s", (123,))
        adapter.close()
    
    @patch('psycopg2.connect')
    def test_query_with_params_dict(self, mock_connect):
        """Test parameterized query with dict parameters"""
        mock_conn = Mock()
        mock_conn.closed = False
        mock_cursor = Mock()
        mock_cursor.description = [('name',), ('age',)]
        mock_cursor.fetchall.return_value = [
            {'name': 'Bob', 'age': 25}
        ]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        adapter = PostgresAdapter(connection_string=POSTGRES_CONN_STRING)
        params = {'user_id': 123, 'min_age': 20}
        result = adapter.query("SELECT name, age FROM users WHERE id = %(user_id)s AND age > %(min_age)s", params)
        
        self.assertIsInstance(result, str)
        self.assertIn("bob", result.lower())
        mock_cursor.execute.assert_called_once_with(
            "SELECT name, age FROM users WHERE id = %(user_id)s AND age > %(min_age)s", 
            params
        )
        adapter.close()
    
    @patch('psycopg2.connect')
    def test_query_with_params_list(self, mock_connect):
        """Test parameterized query with list parameters"""
        mock_conn = Mock()
        mock_conn.closed = False
        mock_cursor = Mock()
        mock_cursor.description = [('name',)]
        mock_cursor.fetchall.return_value = [
            {'name': 'Charlie'}
        ]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        adapter = PostgresAdapter(connection_string=POSTGRES_CONN_STRING)
        result = adapter.query("SELECT name FROM users WHERE id = %s", [456])
        
        self.assertIsInstance(result, str)
        mock_cursor.execute.assert_called_once_with("SELECT name FROM users WHERE id = %s", [456])
        adapter.close()
    
    @patch('psycopg2.connect')
    def test_execute_with_params(self, mock_connect):
        """Test execute method with parameters"""
        mock_conn = Mock()
        mock_conn.closed = False
        mock_cursor = Mock()
        mock_cursor.description = [('count',)]
        mock_cursor.fetchall.return_value = [{'count': 5}]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        adapter = PostgresAdapter(connection_string=POSTGRES_CONN_STRING)
        result = adapter.execute("SELECT COUNT(*) as count FROM users WHERE age > %s", (30,))
        
        self.assertIsInstance(result, str)
        mock_cursor.execute.assert_called_once_with("SELECT COUNT(*) as count FROM users WHERE age > %s", (30,))
        adapter.close()
    
    def test_clean_value_decimal(self):
        """Test cleaning Decimal values"""
        # Create a mock connection that passes validation
        mock_conn = Mock()
        mock_conn.__class__ = psycopg2.extensions.connection
        mock_conn.closed = False
        adapter = PostgresAdapter(connection=mock_conn)
        
        value = Decimal('99.99')
        cleaned = adapter._clean_value(value)
        self.assertEqual(cleaned, 99.99)
        self.assertIsInstance(cleaned, float)
    
    def test_clean_value_datetime(self):
        """Test cleaning datetime values"""
        mock_conn = Mock()
        mock_conn.__class__ = psycopg2.extensions.connection
        mock_conn.closed = False
        adapter = PostgresAdapter(connection=mock_conn)
        
        dt = datetime(2024, 1, 15, 10, 30, 45)
        cleaned = adapter._clean_value(dt)
        self.assertEqual(cleaned, "2024-01-15T10:30:45")
    
    def test_clean_value_uuid(self):
        """Test cleaning UUID values"""
        mock_conn = Mock()
        mock_conn.__class__ = psycopg2.extensions.connection
        mock_conn.closed = False
        adapter = PostgresAdapter(connection=mock_conn)
        
        test_uuid = uuid.uuid4()
        cleaned = adapter._clean_value(test_uuid)
        self.assertEqual(cleaned, str(test_uuid))
    
    def test_clean_value_bytes(self):
        """Test cleaning bytea values"""
        mock_conn = Mock()
        mock_conn.__class__ = psycopg2.extensions.connection
        mock_conn.closed = False
        adapter = PostgresAdapter(connection=mock_conn)
        
        data = b"binary data"
        cleaned = adapter._clean_value(data)
        self.assertIsInstance(cleaned, str)
        # Should be base64 encoded
        import base64
        decoded = base64.b64decode(cleaned)
        self.assertEqual(decoded, data)
    
    def test_clean_value_array(self):
        """Test cleaning array values"""
        mock_conn = Mock()
        mock_conn.__class__ = psycopg2.extensions.connection
        mock_conn.closed = False
        adapter = PostgresAdapter(connection=mock_conn)
        
        arr = [1, 2, 3]
        cleaned = adapter._clean_value(arr)
        self.assertEqual(cleaned, [1, 2, 3])
    
    def test_clean_value_nested(self):
        """Test cleaning nested structures"""
        mock_conn = Mock()
        mock_conn.__class__ = psycopg2.extensions.connection
        mock_conn.closed = False
        adapter = PostgresAdapter(connection=mock_conn)
        
        nested = {
            'decimal': Decimal('10.5'),
            'datetime': datetime(2024, 1, 1),
            'array': [Decimal('1.1'), Decimal('2.2')]
        }
        cleaned = adapter._clean_value(nested)
        self.assertEqual(cleaned['decimal'], 10.5)
        self.assertEqual(cleaned['datetime'], "2024-01-01T00:00:00")
        self.assertEqual(cleaned['array'], [1.1, 2.2])
    
    @patch('psycopg2.connect')
    def test_validate_table_name(self, mock_connect):
        """Test table name validation"""
        mock_conn = Mock()
        mock_conn.closed = False
        mock_connect.return_value = mock_conn
        adapter = PostgresAdapter(connection_string=POSTGRES_CONN_STRING)
        
        # Valid table names
        adapter._validate_table_name("users")
        adapter._validate_table_name("users_table")
        adapter._validate_table_name("schema.users")
        
        # Invalid table names
        from toonpy.adapters.exceptions import SecurityError
        with self.assertRaises(SecurityError):
            adapter._validate_table_name("users; DROP TABLE users;--")
        with self.assertRaises(SecurityError):
            adapter._validate_table_name("users' OR '1'='1")
        adapter.close()
    
    @patch('psycopg2.connect')
    def test_generate_insert_sql_single_row(self, mock_connect):
        """Test INSERT SQL generation for single row"""
        mock_conn = Mock()
        mock_conn.closed = False
        mock_connect.return_value = mock_conn
        adapter = PostgresAdapter(connection_string=POSTGRES_CONN_STRING)
        
        # Mock get_schema to avoid actual DB call
        adapter.get_schema = Mock(return_value={"users": {"columns": [
            {"column_name": "name", "data_type": "character varying"},
            {"column_name": "age", "data_type": "integer"}
        ]}})
        
        data = {"name": "Alice", "age": 30}
        sql, params = adapter._generate_insert_sql("users", data)
        
        self.assertIn("INSERT INTO", sql)
        self.assertIn("users", sql)
        self.assertIn("name", sql)
        self.assertIn("age", sql)
        self.assertEqual(len(params), 2)
        self.assertEqual(params, ["Alice", 30])
        adapter.close()
    
    @patch('psycopg2.connect')
    def test_generate_insert_sql_multiple_rows(self, mock_connect):
        """Test INSERT SQL generation for multiple rows"""
        mock_conn = Mock()
        mock_conn.closed = False
        mock_connect.return_value = mock_conn
        adapter = PostgresAdapter(connection_string=POSTGRES_CONN_STRING)
        
        # Mock get_schema to avoid actual DB call
        adapter.get_schema = Mock(return_value={"users": {"columns": [
            {"column_name": "name", "data_type": "character varying"},
            {"column_name": "age", "data_type": "integer"}
        ]}})
        
        data = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25}
        ]
        sql, params = adapter._generate_insert_sql("users", data)
        
        self.assertIn("INSERT INTO", sql)
        self.assertIn("VALUES", sql)
        self.assertEqual(len(params), 4)  # 2 rows * 2 columns
        self.assertEqual(params, ["Alice", 30, "Bob", 25])
        adapter.close()
    
    @patch('psycopg2.connect')
    def test_generate_update_sql(self, mock_connect):
        """Test UPDATE SQL generation"""
        mock_conn = Mock()
        mock_conn.closed = False
        mock_connect.return_value = mock_conn
        adapter = PostgresAdapter(connection_string=POSTGRES_CONN_STRING)
        
        # Mock get_schema to avoid actual DB call
        adapter.get_schema = Mock(return_value={"users": {"columns": [
            {"column_name": "age", "data_type": "integer"},
            {"column_name": "id", "data_type": "integer"}
        ]}})
        
        data = {"age": 31}
        where = {"id": 123}
        sql, params = adapter._generate_update_sql("users", data, where)
        
        self.assertIn("UPDATE", sql)
        self.assertIn("users", sql)
        self.assertIn("SET", sql)
        self.assertIn("WHERE", sql)
        self.assertEqual(len(params), 2)
        self.assertEqual(params, [31, 123])
        adapter.close()
    
    @patch('psycopg2.connect')
    def test_generate_delete_sql(self, mock_connect):
        """Test DELETE SQL generation"""
        mock_conn = Mock()
        mock_conn.closed = False
        mock_connect.return_value = mock_conn
        adapter = PostgresAdapter(connection_string=POSTGRES_CONN_STRING)
        
        # Mock get_schema to avoid actual DB call
        adapter.get_schema = Mock(return_value={"users": {"columns": [
            {"column_name": "status", "data_type": "character varying"}
        ]}})
        
        where = {"status": "inactive"}
        sql, params = adapter._generate_delete_sql("users", where)
        
        self.assertIn("DELETE FROM", sql)
        self.assertIn("users", sql)
        self.assertIn("WHERE", sql)
        self.assertEqual(len(params), 1)
        self.assertEqual(params, ["inactive"])
        adapter.close()
    
    @patch('psycopg2.connect')
    def test_insert_one_from_toon(self, mock_connect):
        """Test insert_one_from_toon method"""
        from toonpy.core.converter import to_toon
        
        mock_conn = Mock()
        mock_conn.closed = False
        mock_cursor = Mock()
        mock_cursor.rowcount = 1
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        adapter = PostgresAdapter(connection_string=POSTGRES_CONN_STRING)
        
        # Mock get_schema to avoid actual DB call
        adapter.get_schema = Mock(return_value={"users": {"columns": [
            {"column_name": "name", "data_type": "character varying"},
            {"column_name": "age", "data_type": "integer"}
        ]}})
        
        document = {"name": "Test User", "age": 25}
        toon_string = to_toon([document])
        result = adapter.insert_one_from_toon("users", toon_string)
        
        self.assertIsInstance(result, str)
        self.assertIn("rowcount", result.lower())
        mock_cursor.execute.assert_called_once()
        adapter.close()
    
    @patch('psycopg2.connect')
    def test_insert_many_from_toon(self, mock_connect):
        """Test insert_many_from_toon method"""
        from toonpy.core.converter import to_toon
        
        mock_conn = Mock()
        mock_conn.closed = False
        mock_cursor = Mock()
        mock_cursor.rowcount = 2
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        adapter = PostgresAdapter(connection_string=POSTGRES_CONN_STRING)
        
        # Mock get_schema
        adapter.get_schema = Mock(return_value={"users": {"columns": [
            {"column_name": "name", "data_type": "character varying"},
            {"column_name": "age", "data_type": "integer"}
        ]}})
        
        documents = [
            {"name": "User 1", "age": 25},
            {"name": "User 2", "age": 30}
        ]
        toon_string = to_toon(documents)
        result = adapter.insert_many_from_toon("users", toon_string)
        
        self.assertIsInstance(result, str)
        self.assertIn("rowcount", result.lower())
        mock_cursor.execute.assert_called_once()
        adapter.close()
    
    @patch('psycopg2.connect')
    def test_update_from_toon(self, mock_connect):
        """Test update_from_toon method"""
        from toonpy.core.converter import to_toon
        
        mock_conn = Mock()
        mock_conn.closed = False
        mock_cursor = Mock()
        mock_cursor.rowcount = 1
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        adapter = PostgresAdapter(connection_string=POSTGRES_CONN_STRING)
        
        # Mock get_schema
        adapter.get_schema = Mock(return_value={"users": {"columns": [
            {"column_name": "age", "data_type": "integer"},
            {"column_name": "id", "data_type": "integer"}
        ]}})
        
        update_data = {"age": 31}
        toon_string = to_toon([update_data])
        result = adapter.update_from_toon("users", toon_string, where={"id": 123})
        
        self.assertIsInstance(result, str)
        self.assertIn("rowcount", result.lower())
        mock_cursor.execute.assert_called_once()
        adapter.close()
    
    @patch('psycopg2.connect')
    def test_delete_from_toon(self, mock_connect):
        """Test delete_from_toon method"""
        mock_conn = Mock()
        mock_conn.closed = False
        mock_cursor = Mock()
        mock_cursor.rowcount = 1
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        adapter = PostgresAdapter(connection_string=POSTGRES_CONN_STRING)
        
        # Mock get_schema
        adapter.get_schema = Mock(return_value={"users": {"columns": [
            {"column_name": "id", "data_type": "integer"}
        ]}})
        
        result = adapter.delete_from_toon("users", where={"id": 123})
        
        self.assertIsInstance(result, str)
        self.assertIn("rowcount", result.lower())
        mock_cursor.execute.assert_called_once()
        adapter.close()
    
    def test_convert_to_postgres_value_date_string(self):
        """Test converting date string to PostgreSQL date"""
        mock_conn = Mock()
        mock_conn.__class__ = psycopg2.extensions.connection
        mock_conn.closed = False
        adapter = PostgresAdapter(connection=mock_conn)
        
        # Date string
        result = adapter._convert_to_postgres_value("2024-01-15", "date")
        self.assertIsInstance(result, date)
        self.assertEqual(result, date(2024, 1, 15))
    
    def test_convert_to_postgres_value_datetime_string(self):
        """Test converting datetime string to PostgreSQL timestamp"""
        mock_conn = Mock()
        mock_conn.__class__ = psycopg2.extensions.connection
        mock_conn.closed = False
        adapter = PostgresAdapter(connection=mock_conn)
        
        # Datetime string
        result = adapter._convert_to_postgres_value("2024-01-15T10:30:45", "timestamp")
        self.assertIsInstance(result, datetime)
    
    def test_convert_to_postgres_value_uuid_string(self):
        """Test converting UUID string to PostgreSQL UUID"""
        mock_conn = Mock()
        mock_conn.__class__ = psycopg2.extensions.connection
        mock_conn.closed = False
        adapter = PostgresAdapter(connection=mock_conn)
        
        test_uuid_str = str(uuid.uuid4())
        result = adapter._convert_to_postgres_value(test_uuid_str, "uuid")
        self.assertIsInstance(result, uuid.UUID)
        self.assertEqual(str(result), test_uuid_str)
    
    @patch('psycopg2.connect')
    def test_insert_and_query_from_toon(self, mock_connect):
        """Test insert_and_query_from_toon method"""
        from toonpy.core.converter import to_toon
        
        mock_conn = Mock()
        mock_conn.closed = False
        mock_cursor = Mock()
        mock_cursor.description = [('id',), ('name',), ('age',)]
        mock_cursor.fetchone.return_value = (1,)
        mock_cursor.fetchall.return_value = [
            {'id': 1, 'name': 'Test User', 'age': 25}
        ]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        adapter = PostgresAdapter(connection_string=POSTGRES_CONN_STRING)
        
        # Mock get_schema
        adapter.get_schema = Mock(return_value={"users": {"columns": [
            {"column_name": "name", "data_type": "varchar", "is_primary_key": False},
            {"column_name": "age", "data_type": "int", "is_primary_key": False},
            {"column_name": "id", "data_type": "serial", "is_primary_key": True}
        ]}})
        
        document = {"name": "Test User", "age": 25}
        toon_string = to_toon([document])
        result = adapter.insert_and_query_from_toon("users", toon_string)
        
        self.assertIsInstance(result, str)
        self.assertIn("test user", result.lower())
        mock_cursor.execute.assert_called()
        adapter.close()
    
    @patch('psycopg2.connect')
    def test_insert_many_and_query_from_toon(self, mock_connect):
        """Test insert_many_and_query_from_toon method"""
        from toonpy.core.converter import to_toon
        
        mock_conn = Mock()
        mock_conn.closed = False
        mock_cursor = Mock()
        mock_cursor.description = [('id',), ('name',), ('age',)]
        mock_cursor.fetchall.return_value = [
            {'id': 1, 'name': 'User 1', 'age': 25},
            {'id': 2, 'name': 'User 2', 'age': 30}
        ]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        adapter = PostgresAdapter(connection_string=POSTGRES_CONN_STRING)
        
        # Mock get_schema
        adapter.get_schema = Mock(return_value={"users": {"columns": [
            {"column_name": "name", "data_type": "varchar"},
            {"column_name": "age", "data_type": "int"}
        ]}})
        
        documents = [
            {"name": "User 1", "age": 25},
            {"name": "User 2", "age": 30}
        ]
        toon_string = to_toon(documents)
        result = adapter.insert_many_and_query_from_toon("users", toon_string)
        
        self.assertIsInstance(result, str)
        self.assertIn("user 1", result.lower())
        self.assertIn("user 2", result.lower())
        mock_cursor.execute.assert_called()
        adapter.close()
    
    @patch('psycopg2.connect')
    def test_update_and_query_from_toon(self, mock_connect):
        """Test update_and_query_from_toon method"""
        from toonpy.core.converter import to_toon
        
        mock_conn = Mock()
        mock_conn.closed = False
        mock_cursor = Mock()
        mock_cursor.description = [('id',), ('name',), ('age',)]
        mock_cursor.fetchall.return_value = [
            {'id': 123, 'name': 'Alice', 'age': 31, 'status': 'active'}
        ]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        adapter = PostgresAdapter(connection_string=POSTGRES_CONN_STRING)
        
        # Mock get_schema
        adapter.get_schema = Mock(return_value={"users": {"columns": [
            {"column_name": "age", "data_type": "int"},
            {"column_name": "status", "data_type": "varchar"},
            {"column_name": "id", "data_type": "int"}
        ]}})
        
        update_data = {"age": 31, "status": "active"}
        toon_string = to_toon([update_data])
        result = adapter.update_and_query_from_toon("users", toon_string, where={"id": 123})
        
        self.assertIsInstance(result, str)
        self.assertIn("alice", result.lower())
        mock_cursor.execute.assert_called()
        adapter.close()


class TestPostgresAdapterIntegration(unittest.TestCase):
    """Integration tests with Docker PostgreSQL instance"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test database connection"""
        try:
            cls.adapter = PostgresAdapter(connection_string=POSTGRES_CONN_STRING)
        except ConnectionError:
            raise unittest.SkipTest("PostgreSQL not available - skipping integration tests")
    
    def setUp(self):
        """Rollback any failed transactions before each test"""
        try:
            self.adapter.connection.rollback()
        except:
            pass
    
    @classmethod
    def tearDownClass(cls):
        """Close database connection"""
        if hasattr(cls, 'adapter'):
            cls.adapter.close()
    
    def test_connection(self):
        """Test basic connection"""
        self.assertFalse(self.adapter.connection.closed)
    
    def test_query_select(self):
        """Test SELECT query"""
        result = self.adapter.query("SELECT name, email FROM users LIMIT 2")
        self.assertIsInstance(result, str)
        self.assertIn("alice", result.lower())
    
    def test_query_where(self):
        """Test SELECT with WHERE clause"""
        result = self.adapter.query("SELECT name, age FROM users WHERE age > 30")
        self.assertIsInstance(result, str)
    
    def test_query_empty_result(self):
        """Test query with no results"""
        result = self.adapter.query("SELECT * FROM users WHERE age > 200")
        self.assertIsInstance(result, str)
    
    def test_query_join(self):
        """Test SELECT with JOIN"""
        result = self.adapter.query("""
            SELECT u.name, p.name as product_name
            FROM orders o
            JOIN users u ON o.user_id = u.id
            JOIN products p ON o.product_id = p.id
            LIMIT 1
        """)
        self.assertIsInstance(result, str)
    
    def test_query_insert(self):
        """Test INSERT query"""
        result = self.adapter.query("""
            INSERT INTO users (name, email, age, role)
            VALUES ('Test User', 'test@test.com', 25, 'user')
        """)
        self.assertIsInstance(result, str)
        
        # Clean up
        self.adapter.query("DELETE FROM users WHERE email = 'test@test.com'")
    
    def test_query_update(self):
        """Test UPDATE query"""
        # Insert test data
        self.adapter.query("""
            INSERT INTO users (name, email, age, role)
            VALUES ('Update Test', 'update@test.com', 25, 'user')
        """)
        
        # Update
        result = self.adapter.query("""
            UPDATE users SET age = 26 WHERE email = 'update@test.com'
        """)
        self.assertIsInstance(result, str)
        
        # Verify
        verify = self.adapter.query("""
            SELECT age FROM users WHERE email = 'update@test.com'
        """)
        self.assertIn("26", verify)
        
        # Clean up
        self.adapter.query("DELETE FROM users WHERE email = 'update@test.com'")
    
    def test_query_delete(self):
        """Test DELETE query"""
        # Insert test data
        self.adapter.query("""
            INSERT INTO users (name, email, age, role)
            VALUES ('Delete Test', 'delete@test.com', 25, 'user')
        """)
        
        # Delete
        result = self.adapter.query("DELETE FROM users WHERE email = 'delete@test.com'")
        self.assertIsInstance(result, str)
        
        # Verify deleted
        verify = self.adapter.query("""
            SELECT COUNT(*) as count FROM users WHERE email = 'delete@test.com'
        """)
        self.assertIn("0", verify)
    
    def test_get_tables(self):
        """Test get_tables method"""
        tables = self.adapter.get_tables()
        self.assertIsInstance(tables, list)
        self.assertIn('users', tables)
        self.assertIn('products', tables)
        self.assertIn('orders', tables)
    
    def test_get_tables_with_views(self):
        """Test get_tables with include_views"""
        tables = self.adapter.get_tables(include_views=True)
        self.assertIsInstance(tables, list)
        # Should include tables at minimum
        self.assertIn('users', tables)
    
    def test_get_schema_single_table(self):
        """Test get_schema for single table"""
        schema = self.adapter.get_schema('users')
        self.assertIsInstance(schema, dict)
        self.assertIn('users', schema)
        self.assertIn('columns', schema['users'])
        self.assertGreater(len(schema['users']['columns']), 0)
    
    def test_get_schema_all_tables(self):
        """Test get_schema for all tables"""
        schema = self.adapter.get_schema()
        self.assertIsInstance(schema, dict)
        self.assertIn('users', schema)
        self.assertIn('products', schema)
        self.assertIn('orders', schema)
    
    def test_get_schema_invalid_table(self):
        """Test get_schema with invalid table name"""
        with self.assertRaises(SchemaError):
            self.adapter.get_schema('non_existent_table')
    
    def test_query_invalid_sql(self):
        """Test query with invalid SQL"""
        with self.assertRaises(QueryError):
            self.adapter.query("SELECT * FROM non_existent_table")
    
    def test_close(self):
        """Test close method"""
        # Create a new adapter that owns connection
        adapter = PostgresAdapter(connection_string=POSTGRES_CONN_STRING)
        self.assertFalse(adapter.connection.closed)
        adapter.close()
        self.assertTrue(adapter.connection.closed)
    
    def test_execute_alias(self):
        """Test execute method (alias for query)"""
        result1 = self.adapter.query("SELECT COUNT(*) as count FROM users")
        result2 = self.adapter.execute("SELECT COUNT(*) as count FROM users")
        self.assertEqual(result1, result2)
    
    def test_insert_one_from_toon(self):
        """Test insert_one_from_toon integration"""
        from toonpy.core.converter import to_toon
        import time
        
        # Use unique email to avoid conflicts
        unique_email = f"testinsert{int(time.time())}@example.com"
        
        # Clean up any existing test data first
        self.adapter.query("DELETE FROM users WHERE email LIKE 'testinsert%@example.com'")
        
        # Insert test document
        document = {
            "name": "Test Insert User",
            "email": unique_email,
            "age": 28,
            "role": "test"
        }
        toon_string = to_toon([document])
        result = self.adapter.insert_one_from_toon("users", toon_string)
        
        self.assertIsInstance(result, str)
        self.assertIn("rowcount", result.lower())
        
        # Verify insert
        verify = self.adapter.query(
            "SELECT name FROM users WHERE email = %s",
            (unique_email,)
        )
        self.assertIn("test insert user", verify.lower())
        
        # Clean up
        self.adapter.query("DELETE FROM users WHERE email = %s", (unique_email,))
    
    def test_insert_many_from_toon(self):
        """Test insert_many_from_toon integration"""
        from toonpy.core.converter import to_toon
        import time
        
        # Use unique emails to avoid conflicts
        timestamp = int(time.time())
        email1 = f"test1{timestamp}@example.com"
        email2 = f"test2{timestamp}@example.com"
        
        # Clean up any existing test data first
        self.adapter.query("DELETE FROM users WHERE email LIKE 'test1%@example.com' OR email LIKE 'test2%@example.com'")
        
        # Insert test documents
        documents = [
            {"name": "Test User 1", "email": email1, "age": 25, "role": "test"},
            {"name": "Test User 2", "email": email2, "age": 26, "role": "test"}
        ]
        toon_string = to_toon(documents)
        result = self.adapter.insert_many_from_toon("users", toon_string)
        
        self.assertIsInstance(result, str)
        self.assertIn("rowcount", result.lower())
        
        # Verify inserts
        verify = self.adapter.query(
            "SELECT COUNT(*) as count FROM users WHERE email IN (%s, %s)",
            (email1, email2)
        )
        self.assertIn("2", verify)
        
        # Clean up
        self.adapter.query("DELETE FROM users WHERE email IN (%s, %s)", (email1, email2))
    
    def test_update_from_toon(self):
        """Test update_from_toon integration"""
        from toonpy.core.converter import to_toon
        import time
        
        # Use unique email to avoid conflicts
        unique_email = f"updatetest{int(time.time())}@example.com"
        
        # Clean up any existing test data first
        self.adapter.query("DELETE FROM users WHERE email LIKE 'updatetest%@example.com'")
        
        # First insert a test document
        self.adapter.query(
            "INSERT INTO users (name, email, age, role) VALUES (%s, %s, %s, %s)",
            ("Update Test", unique_email, 25, "test")
        )
        
        # Update it
        update_data = {"age": 30, "role": "updated"}
        toon_string = to_toon([update_data])
        result = self.adapter.update_from_toon("users", toon_string, where={"email": unique_email})
        
        self.assertIsInstance(result, str)
        self.assertIn("rowcount", result.lower())
        
        # Verify update
        verify = self.adapter.query(
            "SELECT age FROM users WHERE email = %s",
            (unique_email,)
        )
        self.assertIn("30", verify)
        
        # Clean up
        self.adapter.query("DELETE FROM users WHERE email = %s", (unique_email,))
    
    def test_delete_from_toon(self):
        """Test delete_from_toon integration"""
        # Insert test document
        self.adapter.query("""
            INSERT INTO users (name, email, age, role)
            VALUES ('Delete Test', 'deletetest@example.com', 25, 'test')
        """)
        
        # Delete it
        result = self.adapter.delete_from_toon("users", where={"email": "deletetest@example.com"})
        
        self.assertIsInstance(result, str)
        self.assertIn("rowcount", result.lower())
        
        # Verify deletion
        verify = self.adapter.query(
            "SELECT COUNT(*) as count FROM users WHERE email = %s",
            ("deletetest@example.com",)
        )
        self.assertIn("0", verify)
    
    def test_insert_and_query_from_toon(self):
        """Test insert_and_query_from_toon integration"""
        from toonpy.core.converter import to_toon
        import time
        
        # Use unique email to avoid conflicts
        unique_email = f"roundtrip{int(time.time())}@example.com"
        
        # Clean up any existing test data first
        self.adapter.query("DELETE FROM users WHERE email LIKE 'roundtrip%@example.com'")
        
        # Insert and query back
        document = {
            "name": "Round Trip Test",
            "email": unique_email,
            "age": 28,
            "role": "test"
        }
        toon_string = to_toon([document])
        result = self.adapter.insert_and_query_from_toon("users", toon_string, where={"email": unique_email})
        
        self.assertIsInstance(result, str)
        self.assertIn("round trip test", result.lower())
        self.assertIn(unique_email, result.lower())
        
        # Verify it's actual row data (not just operation result)
        from toonpy.core.converter import from_toon
        result_data = from_toon(result)
        self.assertGreater(len(result_data), 0)
        self.assertEqual(result_data[0]["email"], unique_email)
        
        # Clean up
        self.adapter.query("DELETE FROM users WHERE email = %s", (unique_email,))
    
    def test_insert_many_and_query_from_toon(self):
        """Test insert_many_and_query_from_toon integration"""
        from toonpy.core.converter import to_toon
        import time
        
        # Use unique emails to avoid conflicts
        timestamp = int(time.time())
        email1 = f"roundtrip1{timestamp}@example.com"
        email2 = f"roundtrip2{timestamp}@example.com"
        
        # Clean up any existing test data first
        self.adapter.query("DELETE FROM users WHERE email LIKE 'roundtrip1%@example.com' OR email LIKE 'roundtrip2%@example.com'")
        
        # Insert multiple and query back using WHERE with one email
        documents = [
            {"name": "Round Trip 1", "email": email1, "age": 25, "role": "test"},
            {"name": "Round Trip 2", "email": email2, "age": 26, "role": "test"}
        ]
        toon_string = to_toon(documents)
        # Query back by first email
        result = self.adapter.insert_many_and_query_from_toon("users", toon_string, where={"email": email1})
        
        self.assertIsInstance(result, str)
        self.assertIn("round trip 1", result.lower())
        
        # Verify it's actual row data
        from toonpy.core.converter import from_toon
        result_data = from_toon(result)
        self.assertGreater(len(result_data), 0)
        
        # Clean up
        self.adapter.query("DELETE FROM users WHERE email IN (%s, %s)", (email1, email2))
    
    def test_update_and_query_from_toon(self):
        """Test update_and_query_from_toon integration"""
        from toonpy.core.converter import to_toon
        import time
        
        # Use unique email to avoid conflicts
        unique_email = f"updateroundtrip{int(time.time())}@example.com"
        
        # Clean up any existing test data first
        self.adapter.query("DELETE FROM users WHERE email LIKE 'updateroundtrip%@example.com'")
        
        # First insert a test row
        self.adapter.query(
            "INSERT INTO users (name, email, age, role) VALUES (%s, %s, %s, %s)",
            ("Update Round Trip", unique_email, 25, "test")
        )
        
        # Update and query back
        update_data = {"age": 35}
        toon_string = to_toon([update_data])
        result = self.adapter.update_and_query_from_toon("users", toon_string, where={"email": unique_email})
        
        self.assertIsInstance(result, str)
        self.assertIn("update round trip", result.lower())
        
        # Verify update was applied
        from toonpy.core.converter import from_toon
        result_data = from_toon(result)
        self.assertGreater(len(result_data), 0)
        # Verify age was updated
        verify = self.adapter.query("SELECT age FROM users WHERE email = %s", (unique_email,))
        self.assertIn("35", verify)
        
        # Clean up
        self.adapter.query("DELETE FROM users WHERE email = %s", (unique_email,))


if __name__ == '__main__':
    unittest.main()

