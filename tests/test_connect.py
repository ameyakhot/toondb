"""
Tests for unified connect() function
Includes both unit tests (with mocks) and integration tests (with Docker instances)
"""
import unittest
from unittest.mock import Mock, patch, MagicMock
from pymongo import MongoClient

from toonpy import connect, ConnectionError
from toonpy.adapters.postgres_adapter import PostgresAdapter
from toonpy.adapters.mysql_adapter import MySQLAdapter
from toonpy.adapters.mongo_adapter import MongoAdapter

# Connection strings for Docker instances
POSTGRES_CONN_STRING = "postgresql://testuser:testpass@localhost:5432/testdb"
MYSQL_CONN_STRING = "mysql://testuser:testpass@localhost:3306/testdb"
MONGO_CONN_STRING = "mongodb://localhost:27017"
MONGO_DATABASE = "testdb"


class TestConnectUnit(unittest.TestCase):
    """Unit tests with mocked adapters"""
    
    @patch('toonpy.PostgresAdapter')
    def test_auto_detect_postgresql(self, mock_adapter_class):
        """Test auto-detection of PostgreSQL from connection string"""
        mock_adapter = Mock()
        mock_adapter_class.return_value = mock_adapter
        
        result = connect("postgresql://user:pass@localhost:5432/mydb")
        
        mock_adapter_class.assert_called_once_with(
            connection_string="postgresql://user:pass@localhost:5432/mydb"
        )
        self.assertEqual(result, mock_adapter)
    
    @patch('toonpy.PostgresAdapter')
    def test_auto_detect_postgres_variant(self, mock_adapter_class):
        """Test auto-detection of PostgreSQL with postgres:// prefix"""
        mock_adapter = Mock()
        mock_adapter_class.return_value = mock_adapter
        
        result = connect("postgres://user:pass@localhost:5432/mydb")
        
        mock_adapter_class.assert_called_once_with(
            connection_string="postgres://user:pass@localhost:5432/mydb"
        )
        self.assertEqual(result, mock_adapter)
    
    @patch('toonpy.MySQLAdapter')
    def test_auto_detect_mysql(self, mock_adapter_class):
        """Test auto-detection of MySQL from connection string"""
        mock_adapter = Mock()
        mock_adapter_class.return_value = mock_adapter
        
        result = connect("mysql://user:pass@localhost:3306/mydb")
        
        mock_adapter_class.assert_called_once_with(
            connection_string="mysql://user:pass@localhost:3306/mydb"
        )
        self.assertEqual(result, mock_adapter)
    
    @patch('toonpy.MySQLAdapter')
    def test_auto_detect_mysql_pymysql(self, mock_adapter_class):
        """Test auto-detection of MySQL with mysql+pymysql:// prefix"""
        mock_adapter = Mock()
        mock_adapter_class.return_value = mock_adapter
        
        result = connect("mysql+pymysql://user:pass@localhost:3306/mydb")
        
        mock_adapter_class.assert_called_once_with(
            connection_string="mysql+pymysql://user:pass@localhost:3306/mydb"
        )
        self.assertEqual(result, mock_adapter)
    
    @patch('toonpy.MongoAdapter')
    def test_auto_detect_mongodb(self, mock_adapter_class):
        """Test auto-detection of MongoDB from connection string"""
        mock_adapter = Mock()
        mock_adapter_class.return_value = mock_adapter
        
        result = connect("mongodb://localhost:27017", database="mydb", collection_name="users")
        
        mock_adapter_class.assert_called_once_with(
            connection_string="mongodb://localhost:27017",
            database="mydb",
            collection_name="users",
            collection=None
        )
        self.assertEqual(result, mock_adapter)
    
    @patch('toonpy.MongoAdapter')
    def test_auto_detect_mongodb_srv(self, mock_adapter_class):
        """Test auto-detection of MongoDB with mongodb+srv:// prefix"""
        mock_adapter = Mock()
        mock_adapter_class.return_value = mock_adapter
        
        result = connect("mongodb+srv://cluster.mongodb.net", database="mydb", collection_name="users")
        
        mock_adapter_class.assert_called_once_with(
            connection_string="mongodb+srv://cluster.mongodb.net",
            database="mydb",
            collection_name="users",
            collection=None
        )
        self.assertEqual(result, mock_adapter)
    
    def test_case_insensitive_detection(self):
        """Test that connection string detection is case-insensitive"""
        with patch('toonpy.PostgresAdapter') as mock_adapter_class:
            mock_adapter = Mock()
            mock_adapter_class.return_value = mock_adapter
            
            # Test uppercase
            connect("POSTGRESQL://user:pass@localhost:5432/mydb")
            mock_adapter_class.assert_called()
            
            # Test mixed case
            mock_adapter_class.reset_mock()
            connect("PostgreSQL://user:pass@localhost:5432/mydb")
            mock_adapter_class.assert_called()
    
    @patch('toonpy.PostgresAdapter')
    def test_explicit_db_type_postgresql(self, mock_adapter_class):
        """Test explicit db_type parameter for PostgreSQL"""
        mock_adapter = Mock()
        mock_adapter_class.return_value = mock_adapter
        
        result = connect("postgres://user:pass@localhost:5432/mydb", db_type="postgresql")
        
        mock_adapter_class.assert_called_once_with(
            connection_string="postgres://user:pass@localhost:5432/mydb"
        )
        self.assertEqual(result, mock_adapter)
    
    @patch('toonpy.PostgresAdapter')
    def test_explicit_db_type_postgres(self, mock_adapter_class):
        """Test explicit db_type='postgres' variant"""
        mock_adapter = Mock()
        mock_adapter_class.return_value = mock_adapter
        
        result = connect("some://url", db_type="postgres")
        
        mock_adapter_class.assert_called_once_with(
            connection_string="some://url"
        )
        self.assertEqual(result, mock_adapter)
    
    @patch('toonpy.MySQLAdapter')
    def test_explicit_db_type_mysql(self, mock_adapter_class):
        """Test explicit db_type parameter for MySQL"""
        mock_adapter = Mock()
        mock_adapter_class.return_value = mock_adapter
        
        result = connect("some://url", db_type="mysql")
        
        mock_adapter_class.assert_called_once_with(
            connection_string="some://url"
        )
        self.assertEqual(result, mock_adapter)
    
    @patch('toonpy.MongoAdapter')
    def test_explicit_db_type_mongodb(self, mock_adapter_class):
        """Test explicit db_type parameter for MongoDB"""
        mock_adapter = Mock()
        mock_adapter_class.return_value = mock_adapter
        
        result = connect("some://url", db_type="mongodb", database="mydb", collection_name="users")
        
        mock_adapter_class.assert_called_once_with(
            connection_string="some://url",
            database="mydb",
            collection_name="users",
            collection=None
        )
        self.assertEqual(result, mock_adapter)
    
    @patch('toonpy.PostgresAdapter')
    def test_individual_parameters_postgresql(self, mock_adapter_class):
        """Test connection with individual parameters for PostgreSQL"""
        mock_adapter = Mock()
        mock_adapter_class.return_value = mock_adapter
        
        result = connect(
            db_type="postgresql",
            host="localhost",
            port=5432,
            user="user",
            password="pass",
            database="mydb"
        )
        
        mock_adapter_class.assert_called_once_with(
            connection_string=None,
            host="localhost",
            port=5432,
            user="user",
            password="pass",
            database="mydb"
        )
        self.assertEqual(result, mock_adapter)
    
    @patch('toonpy.MySQLAdapter')
    def test_individual_parameters_mysql(self, mock_adapter_class):
        """Test connection with individual parameters for MySQL"""
        mock_adapter = Mock()
        mock_adapter_class.return_value = mock_adapter
        
        result = connect(
            db_type="mysql",
            host="localhost",
            port=3306,
            user="user",
            password="pass",
            database="mydb"
        )
        
        mock_adapter_class.assert_called_once_with(
            connection_string=None,
            host="localhost",
            port=3306,
            user="user",
            password="pass",
            database="mydb"
        )
        self.assertEqual(result, mock_adapter)
    
    def test_mongodb_missing_database(self):
        """Test MongoDB connection without database parameter"""
        with self.assertRaises(ValueError) as context:
            connect("mongodb://localhost:27017", db_type="mongodb", collection_name="users")
        
        self.assertIn("database", str(context.exception).lower())
        self.assertIn("collection_name", str(context.exception).lower())
    
    def test_mongodb_missing_collection_name(self):
        """Test MongoDB connection without collection_name parameter"""
        with self.assertRaises(ValueError) as context:
            connect("mongodb://localhost:27017", db_type="mongodb", database="mydb")
        
        self.assertIn("database", str(context.exception).lower())
        self.assertIn("collection_name", str(context.exception).lower())
    
    @patch('toonpy.MongoAdapter')
    def test_mongodb_with_collection_object(self, mock_adapter_class):
        """Test MongoDB connection with collection object"""
        mock_adapter = Mock()
        mock_adapter_class.return_value = mock_adapter
        mock_collection = Mock()
        
        result = connect(db_type="mongodb", collection=mock_collection)
        
        mock_adapter_class.assert_called_once_with(
            connection_string=None,
            database=None,
            collection_name=None,
            collection=mock_collection
        )
        self.assertEqual(result, mock_adapter)
    
    def test_unrecognized_connection_string(self):
        """Test error handling for unrecognized connection strings"""
        # SQLite
        with self.assertRaises(ValueError) as context:
            connect("sqlite:///path/to/db.db")
        self.assertIn("SQLite", str(context.exception))
        
        # Oracle
        with self.assertRaises(ValueError) as context:
            connect("oracle://user:pass@host:1521/sid")
        self.assertIn("Oracle", str(context.exception))
        
        # SQL Server
        with self.assertRaises(ValueError) as context:
            connect("mssql://user:pass@host:1433/db")
        self.assertIn("SQL Server", str(context.exception))
        
        # Redis
        with self.assertRaises(ValueError) as context:
            connect("redis://localhost:6379")
        self.assertIn("Redis", str(context.exception))
        
        # HTTP URL
        with self.assertRaises(ValueError) as context:
            connect("https://api.example.com/database")
        self.assertIn("HTTP", str(context.exception))
        
        # JDBC
        with self.assertRaises(ValueError) as context:
            connect("jdbc:postgresql://localhost:5432/mydb")
        self.assertIn("JDBC", str(context.exception))
        
        # Missing protocol
        with self.assertRaises(ValueError) as context:
            connect("localhost:5432/mydb")
        self.assertIn("protocol", str(context.exception).lower())
        
        # Generic unrecognized
        with self.assertRaises(ValueError) as context:
            connect("unknown://user:pass@host/db")
        self.assertIn("Unrecognized", str(context.exception))
    
    def test_missing_connection_string_and_type(self):
        """Test error when both connection_string and db_type are missing"""
        with self.assertRaises(ValueError) as context:
            connect()
        
        self.assertIn("Could not determine database type", str(context.exception))
    
    def test_unsupported_db_type(self):
        """Test error for unsupported database type"""
        with self.assertRaises(ValueError) as context:
            connect("some://url", db_type="sqlite")
        
        self.assertIn("Unsupported database type", str(context.exception))
        self.assertIn("sqlite", str(context.exception))


class TestConnectIntegration(unittest.TestCase):
    """Integration tests with Docker instances"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test connections before all tests"""
        cls.postgres_available = False
        cls.mysql_available = False
        cls.mongo_available = False
        
        # Test PostgreSQL connection
        try:
            adapter = PostgresAdapter(connection_string=POSTGRES_CONN_STRING)
            adapter.query("SELECT 1")
            adapter.close()
            cls.postgres_available = True
        except Exception:
            pass
        
        # Test MySQL connection
        try:
            adapter = MySQLAdapter(connection_string=MYSQL_CONN_STRING)
            adapter.query("SELECT 1")
            adapter.close()
            cls.mysql_available = True
        except Exception:
            pass
        
        # Test MongoDB connection
        try:
            client = MongoClient(MONGO_CONN_STRING)
            client.admin.command('ping')
            client.close()
            cls.mongo_available = True
        except Exception:
            pass
    
    def setUp(self):
        """Set up before each test"""
        pass
    
    def test_postgresql_connection(self):
        """Test connecting to PostgreSQL via connect()"""
        if not self.postgres_available:
            self.skipTest("PostgreSQL not available")
        
        adapter = connect(POSTGRES_CONN_STRING)
        self.assertIsInstance(adapter, PostgresAdapter)
        
        result = adapter.query("SELECT name FROM users LIMIT 1")
        self.assertIsInstance(result, str)
        self.assertIn("alice", result.lower())
        
        adapter.close()
    
    def test_mysql_connection(self):
        """Test connecting to MySQL via connect()"""
        if not self.mysql_available:
            self.skipTest("MySQL not available")
        
        adapter = connect(MYSQL_CONN_STRING)
        self.assertIsInstance(adapter, MySQLAdapter)
        
        result = adapter.query("SELECT name FROM users LIMIT 1")
        self.assertIsInstance(result, str)
        self.assertIn("alice", result.lower())
        
        adapter.close()
    
    def test_mongodb_connection(self):
        """Test connecting to MongoDB via connect()"""
        if not self.mongo_available:
            self.skipTest("MongoDB not available")
        
        adapter = connect(
            MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        self.assertIsInstance(adapter, MongoAdapter)
        
        result = adapter.find({"name": {"$regex": "Alice", "$options": "i"}})
        self.assertIsInstance(result, str)
        self.assertIn("alice", result.lower())
        
        adapter.close()
    
    def test_postgresql_explicit_type(self):
        """Test PostgreSQL with explicit db_type"""
        if not self.postgres_available:
            self.skipTest("PostgreSQL not available")
        
        adapter = connect(POSTGRES_CONN_STRING, db_type="postgresql")
        self.assertIsInstance(adapter, PostgresAdapter)
        adapter.close()
    
    def test_mysql_explicit_type(self):
        """Test MySQL with explicit db_type"""
        if not self.mysql_available:
            self.skipTest("MySQL not available")
        
        adapter = connect(MYSQL_CONN_STRING, db_type="mysql")
        self.assertIsInstance(adapter, MySQLAdapter)
        adapter.close()
    
    def test_mongodb_explicit_type(self):
        """Test MongoDB with explicit db_type"""
        if not self.mongo_available:
            self.skipTest("MongoDB not available")
        
        adapter = connect(
            MONGO_CONN_STRING,
            db_type="mongodb",
            database=MONGO_DATABASE,
            collection_name="users"
        )
        self.assertIsInstance(adapter, MongoAdapter)
        adapter.close()
    
    def test_postgresql_individual_params(self):
        """Test PostgreSQL with individual parameters"""
        if not self.postgres_available:
            self.skipTest("PostgreSQL not available")
        
        adapter = connect(
            db_type="postgresql",
            host="localhost",
            port=5432,
            user="testuser",
            password="testpass",
            database="testdb"
        )
        self.assertIsInstance(adapter, PostgresAdapter)
        
        result = adapter.query("SELECT COUNT(*) as count FROM users")
        self.assertIsInstance(result, str)
        
        adapter.close()
    
    def test_mysql_individual_params(self):
        """Test MySQL with individual parameters"""
        if not self.mysql_available:
            self.skipTest("MySQL not available")
        
        adapter = connect(
            db_type="mysql",
            host="localhost",
            port=3306,
            user="testuser",
            password="testpass",
            database="testdb"
        )
        self.assertIsInstance(adapter, MySQLAdapter)
        
        result = adapter.query("SELECT COUNT(*) as count FROM users")
        self.assertIsInstance(result, str)
        
        adapter.close()
    
    def test_query_execution_through_connect(self):
        """Test that queries work correctly through connect() returned adapters"""
        if not self.postgres_available:
            self.skipTest("PostgreSQL not available")
        
        adapter = connect(POSTGRES_CONN_STRING)
        
        # Test SELECT query
        result = adapter.query("SELECT name, email FROM users WHERE age > 25 LIMIT 2")
        self.assertIsInstance(result, str)
        self.assertIn("alice", result.lower())
        
        adapter.close()


if __name__ == "__main__":
    unittest.main()

