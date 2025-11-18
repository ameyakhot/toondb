"""
Unified Connection Function Examples
Demonstrates usage of the connect() function for all database types
"""
from toonpy import connect

# Connection strings for Docker instances
POSTGRES_CONN_STRING = "postgresql://testuser:testpass@localhost:5432/testdb"
MYSQL_CONN_STRING = "mysql://testuser:testpass@localhost:3306/testdb"
MONGO_CONN_STRING = "mongodb://localhost:27017"
MONGO_DATABASE = "testdb"


def example_auto_detection():
    """Example: Auto-detection from connection strings"""
    print("=" * 60)
    print("Example 1: Auto-Detection from Connection Strings")
    print("=" * 60)
    
    # PostgreSQL - auto-detected from postgresql:// prefix
    print("\n--- PostgreSQL ---")
    try:
        adapter = connect(POSTGRES_CONN_STRING)
        result = adapter.query("SELECT name, email FROM users LIMIT 2")
        print("Query Result (TOON format):")
        print(result[:200] + "..." if len(result) > 200 else result)
        adapter.close()
    except Exception as e:
        print(f"Error: {e}")
    
    # MySQL - auto-detected from mysql:// prefix
    print("\n--- MySQL ---")
    try:
        adapter = connect(MYSQL_CONN_STRING)
        result = adapter.query("SELECT name, email FROM users LIMIT 2")
        print("Query Result (TOON format):")
        print(result[:200] + "..." if len(result) > 200 else result)
        adapter.close()
    except Exception as e:
        print(f"Error: {e}")
    
    # MongoDB - auto-detected from mongodb:// prefix
    print("\n--- MongoDB ---")
    try:
        adapter = connect(
            MONGO_CONN_STRING,
            database=MONGO_DATABASE,
            collection_name="users"
        )
        result = adapter.find({"role": "admin"})
        print("Query Result (TOON format):")
        print(result[:200] + "..." if len(result) > 200 else result)
        adapter.close()
    except Exception as e:
        print(f"Error: {e}")


def example_explicit_type():
    """Example: Explicit database type specification"""
    print("\n" + "=" * 60)
    print("Example 2: Explicit Database Type")
    print("=" * 60)
    
    # Sometimes you may want to explicitly specify the type
    print("\n--- PostgreSQL with explicit type ---")
    try:
        adapter = connect(POSTGRES_CONN_STRING, db_type="postgresql")
        result = adapter.query("SELECT COUNT(*) as count FROM users")
        print("Query Result:")
        print(result)
        adapter.close()
    except Exception as e:
        print(f"Error: {e}")
    
    print("\n--- MySQL with explicit type ---")
    try:
        adapter = connect(MYSQL_CONN_STRING, db_type="mysql")
        result = adapter.query("SELECT COUNT(*) as count FROM users")
        print("Query Result:")
        print(result)
        adapter.close()
    except Exception as e:
        print(f"Error: {e}")


def example_individual_parameters():
    """Example: Using individual connection parameters"""
    print("\n" + "=" * 60)
    print("Example 3: Individual Connection Parameters")
    print("=" * 60)
    
    print("\n--- PostgreSQL with individual parameters ---")
    try:
        adapter = connect(
            db_type="postgresql",
            host="localhost",
            port=5432,
            user="testuser",
            password="testpass",
            database="testdb"
        )
        result = adapter.query("SELECT name FROM users LIMIT 1")
        print("Query Result:")
        print(result)
        adapter.close()
    except Exception as e:
        print(f"Error: {e}")
    
    print("\n--- MySQL with individual parameters ---")
    try:
        adapter = connect(
            db_type="mysql",
            host="localhost",
            port=3306,
            user="testuser",
            password="testpass",
            database="testdb"
        )
        result = adapter.query("SELECT name FROM users LIMIT 1")
        print("Query Result:")
        print(result)
        adapter.close()
    except Exception as e:
        print(f"Error: {e}")


def example_error_handling():
    """Example: Error handling for unrecognized connection strings"""
    print("\n" + "=" * 60)
    print("Example 4: Error Handling")
    print("=" * 60)
    
    # Test various error scenarios
    error_cases = [
        ("sqlite:///path/to/db.db", "SQLite (unsupported)"),
        ("oracle://user:pass@host:1521/sid", "Oracle (unsupported)"),
        ("mssql://user:pass@host:1433/db", "SQL Server (unsupported)"),
        ("redis://localhost:6379", "Redis (unsupported)"),
        ("https://api.example.com/database", "HTTP URL"),
        ("jdbc:postgresql://localhost:5432/mydb", "JDBC connection string"),
        ("localhost:5432/mydb", "Missing protocol prefix"),
    ]
    
    for conn_string, description in error_cases:
        print(f"\n--- {description} ---")
        try:
            connect(conn_string)
            print("  Unexpected: No error raised")
        except ValueError as e:
            print(f"  Error caught: {str(e)[:100]}...")
        except Exception as e:
            print(f"  Unexpected error type: {type(e).__name__}: {e}")


def example_mongodb_collection_object():
    """Example: MongoDB with collection object"""
    print("\n" + "=" * 60)
    print("Example 5: MongoDB with Collection Object")
    print("=" * 60)
    
    try:
        from pymongo import MongoClient
        
        client = MongoClient(MONGO_CONN_STRING)
        collection = client[MONGO_DATABASE]["users"]
        
        # Use collection object instead of connection string
        adapter = connect(db_type="mongodb", collection=collection)
        result = adapter.find({"age": {"$gt": 25}})
        print("Query Result (first 200 chars):")
        print(result[:200] + "..." if len(result) > 200 else result)
        adapter.close()
        client.close()
    except Exception as e:
        print(f"Error: {e}")


def example_case_insensitive():
    """Example: Case-insensitive connection string detection"""
    print("\n" + "=" * 60)
    print("Example 6: Case-Insensitive Detection")
    print("=" * 60)
    
    # All these should work (case-insensitive)
    variations = [
        "POSTGRESQL://user:pass@localhost:5432/mydb",
        "PostgreSQL://user:pass@localhost:5432/mydb",
        "postgresql://user:pass@localhost:5432/mydb",
        "MYSQL://user:pass@localhost:3306/mydb",
        "MongoDB://localhost:27017",
    ]
    
    print("\nConnection string detection is case-insensitive:")
    for conn_string in variations[:3]:  # Just show first 3
        print(f"  {conn_string[:30]}... → Detected as PostgreSQL")
    for conn_string in variations[3:4]:
        print(f"  {conn_string[:30]}... → Detected as MySQL")
    for conn_string in variations[4:5]:
        print(f"  {conn_string[:30]}... → Detected as MongoDB")


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("Unified Connection Function Examples")
    print("=" * 60)
    
    example_auto_detection()
    example_explicit_type()
    example_individual_parameters()
    example_mongodb_collection_object()
    example_case_insensitive()
    example_error_handling()
    
    print("\n" + "=" * 60)
    print("All examples completed!")
    print("=" * 60)

