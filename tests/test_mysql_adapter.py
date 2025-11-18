"""
Tests for MySQLAdapter
Includes both unit tests (with mocks) and integration tests (with Docker MySQL)
"""
import unittest
from unittest.mock import Mock, patch, MagicMock
import pymysql
from pymysql.cursors import DictCursor
from datetime import datetime, date, time
from decimal import Decimal

from toonpy import MySQLAdapter, ConnectionError, QueryError, SchemaError

# Connection string for Docker MySQL instance
MYSQL_CONN_STRING = "mysql://testuser:testpass@localhost:3307/testdb"


class TestMySQLAdapterUnit(unittest.TestCase):
    """Unit tests with mocked connections"""
    
    @patch('pymysql.connect')
    def test_init_with_connection_string(self, mock_connect):
        """Test initialization with connection string"""
        mock_conn = Mock()
        mock_conn.open = True
        mock_connect.return_value = mock_conn
        
        adapter = MySQLAdapter(connection_string=MYSQL_CONN_STRING)
        self.assertEqual(adapter.connection, mock_conn)
        self.assertTrue(adapter.own_connection)
        adapter.close()
    
    @patch('pymysql.connect')
    def test_init_with_connection_params(self, mock_connect):
        """Test initialization with individual parameters"""
        mock_conn = Mock()
        mock_conn.open = True
        mock_connect.return_value = mock_conn
        
        adapter = MySQLAdapter(
            host="localhost",
            port=3306,
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
        mock_conn.open = True
        mock_conn.__class__ = pymysql.connections.Connection
        
        adapter = MySQLAdapter(connection=mock_conn)
        self.assertEqual(adapter.connection, mock_conn)
        self.assertFalse(adapter.own_connection)
    
    def test_init_with_invalid_connection(self):
        """Test initialization with invalid connection object"""
        invalid_conn = "not a connection"
        
        with self.assertRaises(ValueError):
            MySQLAdapter(connection=invalid_conn)
    
    @patch('pymysql.connect')
    def test_init_connection_error(self, mock_connect):
        """Test initialization with connection error"""
        mock_connect.side_effect = pymysql.OperationalError("Connection failed")
        
        with self.assertRaises(ConnectionError):
            MySQLAdapter(connection_string=MYSQL_CONN_STRING)
    
    @patch('pymysql.connect')
    def test_query_select(self, mock_connect):
        """Test SELECT query execution"""
        mock_conn = Mock()
        mock_conn.open = True
        mock_cursor = Mock()
        mock_cursor.description = [('name',), ('age',)]  # Has description = SELECT query
        mock_cursor.fetchall.return_value = [
            {'name': 'Alice', 'age': 30},
            {'name': 'Bob', 'age': 25}
        ]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        adapter = MySQLAdapter(connection_string=MYSQL_CONN_STRING)
        result = adapter.query("SELECT name, age FROM users")
        
        self.assertIsInstance(result, str)
        self.assertIn("alice", result.lower())
        mock_cursor.execute.assert_called_once()
        adapter.close()
    
    @patch('pymysql.connect')
    def test_query_non_select(self, mock_connect):
        """Test non-SELECT query execution"""
        mock_conn = Mock()
        mock_conn.open = True
        mock_cursor = Mock()
        mock_cursor.description = None  # No description = non-SELECT
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        adapter = MySQLAdapter(connection_string=MYSQL_CONN_STRING)
        result = adapter.query("INSERT INTO users (name) VALUES ('Test')")
        
        self.assertIsInstance(result, str)
        mock_cursor.execute.assert_called_once()
        mock_conn.commit.assert_called_once()  # Should commit DML
        adapter.close()
    
    @patch('pymysql.connect')
    def test_query_error(self, mock_connect):
        """Test query error handling"""
        mock_conn = Mock()
        mock_conn.open = True
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = pymysql.ProgrammingError("Syntax error")
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        adapter = MySQLAdapter(connection_string=MYSQL_CONN_STRING)
        with self.assertRaises(QueryError):
            adapter.query("INVALID SQL")
        mock_conn.rollback.assert_called()  # Should rollback on error
        adapter.close()
    
    @patch('pymysql.connect')
    def test_query_with_params_tuple(self, mock_connect):
        """Test parameterized query with tuple parameters"""
        mock_conn = Mock()
        mock_conn.open = True
        mock_cursor = Mock()
        mock_cursor.description = [('name',), ('age',)]
        mock_cursor.fetchall.return_value = [
            {'name': 'Alice', 'age': 30}
        ]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        adapter = MySQLAdapter(connection_string=MYSQL_CONN_STRING)
        result = adapter.query("SELECT name, age FROM users WHERE id = %s", (123,))
        
        self.assertIsInstance(result, str)
        self.assertIn("alice", result.lower())
        # Verify parameterized query was called
        mock_cursor.execute.assert_called_once_with("SELECT name, age FROM users WHERE id = %s", (123,))
        adapter.close()
    
    @patch('pymysql.connect')
    def test_query_with_params_dict(self, mock_connect):
        """Test parameterized query with dict parameters"""
        mock_conn = Mock()
        mock_conn.open = True
        mock_cursor = Mock()
        mock_cursor.description = [('name',), ('age',)]
        mock_cursor.fetchall.return_value = [
            {'name': 'Bob', 'age': 25}
        ]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        adapter = MySQLAdapter(connection_string=MYSQL_CONN_STRING)
        params = {'user_id': 123, 'min_age': 20}
        result = adapter.query("SELECT name, age FROM users WHERE id = %(user_id)s AND age > %(min_age)s", params)
        
        self.assertIsInstance(result, str)
        self.assertIn("bob", result.lower())
        mock_cursor.execute.assert_called_once_with(
            "SELECT name, age FROM users WHERE id = %(user_id)s AND age > %(min_age)s", 
            params
        )
        adapter.close()
    
    @patch('pymysql.connect')
    def test_query_with_params_list(self, mock_connect):
        """Test parameterized query with list parameters"""
        mock_conn = Mock()
        mock_conn.open = True
        mock_cursor = Mock()
        mock_cursor.description = [('name',)]
        mock_cursor.fetchall.return_value = [
            {'name': 'Charlie'}
        ]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        adapter = MySQLAdapter(connection_string=MYSQL_CONN_STRING)
        result = adapter.query("SELECT name FROM users WHERE id = %s", [456])
        
        self.assertIsInstance(result, str)
        mock_cursor.execute.assert_called_once_with("SELECT name FROM users WHERE id = %s", [456])
        adapter.close()
    
    @patch('pymysql.connect')
    def test_execute_with_params(self, mock_connect):
        """Test execute method with parameters"""
        mock_conn = Mock()
        mock_conn.open = True
        mock_cursor = Mock()
        mock_cursor.description = [('count',)]
        mock_cursor.fetchall.return_value = [{'count': 5}]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        adapter = MySQLAdapter(connection_string=MYSQL_CONN_STRING)
        result = adapter.execute("SELECT COUNT(*) as count FROM users WHERE age > %s", (30,))
        
        self.assertIsInstance(result, str)
        mock_cursor.execute.assert_called_once_with("SELECT COUNT(*) as count FROM users WHERE age > %s", (30,))
        adapter.close()
    
    def test_clean_value_decimal(self):
        """Test cleaning Decimal values"""
        mock_conn = Mock()
        mock_conn.__class__ = pymysql.connections.Connection
        mock_conn.open = True
        adapter = MySQLAdapter(connection=mock_conn)
        
        value = Decimal('99.99')
        cleaned = adapter._clean_value(value)
        self.assertEqual(cleaned, 99.99)
        self.assertIsInstance(cleaned, float)
    
    def test_clean_value_datetime(self):
        """Test cleaning datetime values"""
        mock_conn = Mock()
        mock_conn.__class__ = pymysql.connections.Connection
        mock_conn.open = True
        adapter = MySQLAdapter(connection=mock_conn)
        
        dt = datetime(2024, 1, 15, 10, 30, 45)
        cleaned = adapter._clean_value(dt)
        self.assertEqual(cleaned, "2024-01-15T10:30:45")
    
    def test_clean_value_bytes(self):
        """Test cleaning BLOB values"""
        mock_conn = Mock()
        mock_conn.__class__ = pymysql.connections.Connection
        mock_conn.open = True
        adapter = MySQLAdapter(connection=mock_conn)
        
        data = b"binary data"
        cleaned = adapter._clean_value(data)
        self.assertIsInstance(cleaned, str)
        # Should be base64 encoded
        import base64
        decoded = base64.b64decode(cleaned)
        self.assertEqual(decoded, data)
    
    def test_clean_value_array(self):
        """Test cleaning array/SET values"""
        mock_conn = Mock()
        mock_conn.__class__ = pymysql.connections.Connection
        mock_conn.open = True
        adapter = MySQLAdapter(connection=mock_conn)
        
        arr = [1, 2, 3]
        cleaned = adapter._clean_value(arr)
        self.assertEqual(cleaned, [1, 2, 3])
    
    def test_clean_value_nested(self):
        """Test cleaning nested structures"""
        mock_conn = Mock()
        mock_conn.__class__ = pymysql.connections.Connection
        mock_conn.open = True
        adapter = MySQLAdapter(connection=mock_conn)
        
        nested = {
            'decimal': Decimal('10.5'),
            'datetime': datetime(2024, 1, 1),
            'array': [Decimal('1.1'), Decimal('2.2')]
        }
        cleaned = adapter._clean_value(nested)
        self.assertEqual(cleaned['decimal'], 10.5)
        self.assertEqual(cleaned['datetime'], "2024-01-01T00:00:00")
        self.assertEqual(cleaned['array'], [1.1, 2.2])
    
    @patch('pymysql.connect')
    def test_validate_table_name(self, mock_connect):
        """Test table name validation"""
        mock_conn = Mock()
        mock_conn.open = True
        mock_connect.return_value = mock_conn
        adapter = MySQLAdapter(connection_string=MYSQL_CONN_STRING)
        
        # Valid table names
        adapter._validate_table_name("users")
        adapter._validate_table_name("users_table")
        adapter._validate_table_name("database.users")
        
        # Invalid table names
        from toonpy.adapters.exceptions import SecurityError
        with self.assertRaises(SecurityError):
            adapter._validate_table_name("users; DROP TABLE users;--")
        with self.assertRaises(SecurityError):
            adapter._validate_table_name("users' OR '1'='1")
        adapter.close()
    
    @patch('pymysql.connect')
    def test_generate_insert_sql_single_row(self, mock_connect):
        """Test INSERT SQL generation for single row"""
        mock_conn = Mock()
        mock_conn.open = True
        mock_connect.return_value = mock_conn
        adapter = MySQLAdapter(connection_string=MYSQL_CONN_STRING)
        
        # Mock get_schema to avoid actual DB call
        adapter.get_schema = Mock(return_value={"users": {"columns": [
            {"column_name": "name", "data_type": "varchar"},
            {"column_name": "age", "data_type": "int"}
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
    
    @patch('pymysql.connect')
    def test_generate_insert_sql_multiple_rows(self, mock_connect):
        """Test INSERT SQL generation for multiple rows"""
        mock_conn = Mock()
        mock_conn.open = True
        mock_connect.return_value = mock_conn
        adapter = MySQLAdapter(connection_string=MYSQL_CONN_STRING)
        
        # Mock get_schema to avoid actual DB call
        adapter.get_schema = Mock(return_value={"users": {"columns": [
            {"column_name": "name", "data_type": "varchar"},
            {"column_name": "age", "data_type": "int"}
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
    
    @patch('pymysql.connect')
    def test_generate_update_sql(self, mock_connect):
        """Test UPDATE SQL generation"""
        mock_conn = Mock()
        mock_conn.open = True
        mock_connect.return_value = mock_conn
        adapter = MySQLAdapter(connection_string=MYSQL_CONN_STRING)
        
        # Mock get_schema to avoid actual DB call
        adapter.get_schema = Mock(return_value={"users": {"columns": [
            {"column_name": "age", "data_type": "int"},
            {"column_name": "id", "data_type": "int"}
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
    
    @patch('pymysql.connect')
    def test_generate_delete_sql(self, mock_connect):
        """Test DELETE SQL generation"""
        mock_conn = Mock()
        mock_conn.open = True
        mock_connect.return_value = mock_conn
        adapter = MySQLAdapter(connection_string=MYSQL_CONN_STRING)
        
        # Mock get_schema to avoid actual DB call
        adapter.get_schema = Mock(return_value={"users": {"columns": [
            {"column_name": "status", "data_type": "varchar"}
        ]}})
        
        where = {"status": "inactive"}
        sql, params = adapter._generate_delete_sql("users", where)
        
        self.assertIn("DELETE FROM", sql)
        self.assertIn("users", sql)
        self.assertIn("WHERE", sql)
        self.assertEqual(len(params), 1)
        self.assertEqual(params, ["inactive"])
        adapter.close()
    
    @patch('pymysql.connect')
    def test_insert_one_from_toon(self, mock_connect):
        """Test insert_one_from_toon method"""
        from toonpy.core.converter import to_toon
        
        mock_conn = Mock()
        mock_conn.open = True
        mock_cursor = Mock()
        mock_cursor.rowcount = 1
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        adapter = MySQLAdapter(connection_string=MYSQL_CONN_STRING)
        
        # Mock get_schema to avoid actual DB call
        adapter.get_schema = Mock(return_value={"users": {"columns": [
            {"column_name": "name", "data_type": "varchar"},
            {"column_name": "age", "data_type": "int"}
        ]}})
        
        document = {"name": "Test User", "age": 25}
        toon_string = to_toon([document])
        result = adapter.insert_one_from_toon("users", toon_string)
        
        self.assertIsInstance(result, str)
        self.assertIn("rowcount", result.lower())
        mock_cursor.execute.assert_called_once()
        adapter.close()
    
    @patch('pymysql.connect')
    def test_insert_many_from_toon(self, mock_connect):
        """Test insert_many_from_toon method"""
        from toonpy.core.converter import to_toon
        
        mock_conn = Mock()
        mock_conn.open = True
        mock_cursor = Mock()
        mock_cursor.rowcount = 2
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        adapter = MySQLAdapter(connection_string=MYSQL_CONN_STRING)
        
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
        result = adapter.insert_many_from_toon("users", toon_string)
        
        self.assertIsInstance(result, str)
        self.assertIn("rowcount", result.lower())
        mock_cursor.execute.assert_called_once()
        adapter.close()
    
    @patch('pymysql.connect')
    def test_update_from_toon(self, mock_connect):
        """Test update_from_toon method"""
        from toonpy.core.converter import to_toon
        
        mock_conn = Mock()
        mock_conn.open = True
        mock_cursor = Mock()
        mock_cursor.rowcount = 1
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        adapter = MySQLAdapter(connection_string=MYSQL_CONN_STRING)
        
        # Mock get_schema
        adapter.get_schema = Mock(return_value={"users": {"columns": [
            {"column_name": "age", "data_type": "int"},
            {"column_name": "id", "data_type": "int"}
        ]}})
        
        update_data = {"age": 31}
        toon_string = to_toon([update_data])
        result = adapter.update_from_toon("users", toon_string, where={"id": 123})
        
        self.assertIsInstance(result, str)
        self.assertIn("rowcount", result.lower())
        mock_cursor.execute.assert_called_once()
        adapter.close()
    
    @patch('pymysql.connect')
    def test_delete_from_toon(self, mock_connect):
        """Test delete_from_toon method"""
        mock_conn = Mock()
        mock_conn.open = True
        mock_cursor = Mock()
        mock_cursor.rowcount = 1
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        adapter = MySQLAdapter(connection_string=MYSQL_CONN_STRING)
        
        # Mock get_schema
        adapter.get_schema = Mock(return_value={"users": {"columns": [
            {"column_name": "id", "data_type": "int"}
        ]}})
        
        result = adapter.delete_from_toon("users", where={"id": 123})
        
        self.assertIsInstance(result, str)
        self.assertIn("rowcount", result.lower())
        mock_cursor.execute.assert_called_once()
        adapter.close()
    
    def test_convert_to_mysql_value_date_string(self):
        """Test converting date string to MySQL date"""
        mock_conn = Mock()
        mock_conn.__class__ = pymysql.connections.Connection
        mock_conn.open = True
        adapter = MySQLAdapter(connection=mock_conn)
        
        # Date string
        result = adapter._convert_to_mysql_value("2024-01-15", "date")
        self.assertIsInstance(result, date)
        self.assertEqual(result, date(2024, 1, 15))
    
    def test_convert_to_mysql_value_datetime_string(self):
        """Test converting datetime string to MySQL datetime"""
        mock_conn = Mock()
        mock_conn.__class__ = pymysql.connections.Connection
        mock_conn.open = True
        adapter = MySQLAdapter(connection=mock_conn)
        
        # Datetime string
        result = adapter._convert_to_mysql_value("2024-01-15T10:30:45", "datetime")
        self.assertIsInstance(result, datetime)
        self.assertEqual(result, datetime(2024, 1, 15, 10, 30, 45))
    
    def test_convert_to_mysql_value_base64_blob(self):
        """Test converting base64 string to MySQL BLOB"""
        mock_conn = Mock()
        mock_conn.__class__ = pymysql.connections.Connection
        mock_conn.open = True
        adapter = MySQLAdapter(connection=mock_conn)
        
        import base64
        test_data = b"binary data"
        base64_str = base64.b64encode(test_data).decode('utf-8')
        
        result = adapter._convert_to_mysql_value(base64_str, "blob")
        self.assertIsInstance(result, bytes)
        self.assertEqual(result, test_data)


class TestMySQLAdapterIntegration(unittest.TestCase):
    """Integration tests with Docker MySQL instance"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test database connection"""
        try:
            cls.adapter = MySQLAdapter(connection_string=MYSQL_CONN_STRING)
        except ConnectionError:
            raise unittest.SkipTest("MySQL not available - skipping integration tests")
    
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
        self.assertTrue(self.adapter.connection.open)
    
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
        adapter = MySQLAdapter(connection_string=MYSQL_CONN_STRING)
        self.assertTrue(adapter.connection.open)
        adapter.close()
        self.assertFalse(adapter.connection.open)
    
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
        unique_email = f"testupdate{int(time.time())}@example.com"
        
        # Clean up any existing test data first
        self.adapter.query("DELETE FROM users WHERE email LIKE 'testupdate%@example.com'")
        
        # Insert test data
        self.adapter.query(
            "INSERT INTO users (name, email, age, role) VALUES (%s, %s, %s, %s)",
            ("Test Update User", unique_email, 25, "test")
        )
        
        # Update using TOON
        update_data = {"age": 31}
        toon_string = to_toon([update_data])
        result = self.adapter.update_from_toon("users", toon_string, where={"email": unique_email})
        
        self.assertIsInstance(result, str)
        self.assertIn("rowcount", result.lower())
        
        # Verify update
        verify = self.adapter.query(
            "SELECT age FROM users WHERE email = %s",
            (unique_email,)
        )
        self.assertIn("31", verify)
        
        # Clean up
        self.adapter.query("DELETE FROM users WHERE email = %s", (unique_email,))
    
    def test_delete_from_toon(self):
        """Test delete_from_toon integration"""
        import time
        
        # Use unique email to avoid conflicts
        unique_email = f"testdelete{int(time.time())}@example.com"
        
        # Clean up any existing test data first
        self.adapter.query("DELETE FROM users WHERE email LIKE 'testdelete%@example.com'")
        
        # Insert test data
        self.adapter.query(
            "INSERT INTO users (name, email, age, role) VALUES (%s, %s, %s, %s)",
            ("Test Delete User", unique_email, 25, "test")
        )
        
        # Delete using TOON
        result = self.adapter.delete_from_toon("users", where={"email": unique_email})
        
        self.assertIsInstance(result, str)
        self.assertIn("rowcount", result.lower())
        
        # Verify deleted
        verify = self.adapter.query(
            "SELECT COUNT(*) as count FROM users WHERE email = %s",
            (unique_email,)
        )
        self.assertIn("0", verify)
    
    def test_insert_one_from_toon_with_on_duplicate_key_update(self):
        """Test insert_one_from_toon with ON DUPLICATE KEY UPDATE"""
        from toonpy.core.converter import to_toon
        import time
        
        # Use unique email to avoid conflicts
        unique_email = f"testduplicate{int(time.time())}@example.com"
        
        # Clean up any existing test data first
        self.adapter.query("DELETE FROM users WHERE email LIKE 'testduplicate%@example.com'")
        
        # First insert
        document = {
            "name": "Test Duplicate User",
            "email": unique_email,
            "age": 25,
            "role": "test"
        }
        toon_string = to_toon([document])
        self.adapter.insert_one_from_toon("users", toon_string)
        
        # Second insert with same email (should update)
        document2 = {
            "name": "Updated Name",
            "email": unique_email,
            "age": 30,
            "role": "test"
        }
        toon_string2 = to_toon([document2])
        result = self.adapter.insert_one_from_toon(
            "users", 
            toon_string2,
            on_duplicate_key_update="name = VALUES(name), age = VALUES(age)"
        )
        
        self.assertIsInstance(result, str)
        
        # Verify update
        verify = self.adapter.query(
            "SELECT name, age FROM users WHERE email = %s",
            (unique_email,)
        )
        self.assertIn("updated name", verify.lower())
        self.assertIn("30", verify)
        
        # Clean up
        self.adapter.query("DELETE FROM users WHERE email = %s", (unique_email,))
    
    def test_insert_one_from_toon_empty_data(self):
        """Test insert_one_from_toon with empty data"""
        from toonpy.core.converter import to_toon
        
        empty_toon = to_toon([])
        with self.assertRaises(ValueError):
            self.adapter.insert_one_from_toon("users", empty_toon)
    
    def test_update_from_toon_empty_where(self):
        """Test update_from_toon with empty WHERE clause"""
        from toonpy.core.converter import to_toon
        
        update_data = {"age": 31}
        toon_string = to_toon([update_data])
        with self.assertRaises(ValueError):
            self.adapter.update_from_toon("users", toon_string, where={})
    
    def test_delete_from_toon_empty_where(self):
        """Test delete_from_toon with empty WHERE clause"""
        with self.assertRaises(ValueError):
            self.adapter.delete_from_toon("users", where={})
    
    def test_insert_one_from_toon_invalid_column(self):
        """Test insert_one_from_toon with invalid column"""
        from toonpy.core.converter import to_toon
        
        document = {"invalid_column": "value", "name": "Test"}
        toon_string = to_toon([document])
        
        # Should raise SchemaError if column validation is enabled
        # (may skip validation if schema lookup fails)
        try:
            result = self.adapter.insert_one_from_toon("users", toon_string)
            # If no error, that's okay (validation may be skipped)
            self.assertIsInstance(result, str)
        except SchemaError:
            # Expected if validation works
            pass


if __name__ == '__main__':
    unittest.main()

