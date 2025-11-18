"""
MySQL Adapter Examples
Demonstrates usage of MySQLAdapter with various scenarios
"""
from toondb import MySQLAdapter, ConnectionError, QueryError, SchemaError
import pymysql

# Connection string for Docker MySQL instance
MYSQL_CONN_STRING = "mysql://testuser:testpass@localhost:3306/testdb"


def example_connection_string():
    """Example: Using connection string"""
    print("=" * 60)
    print("Example 1: Using Connection String")
    print("=" * 60)
    
    adapter = MySQLAdapter(connection_string=MYSQL_CONN_STRING)
    
    try:
        # Simple SELECT query
        result = adapter.query("SELECT name, email, age FROM users LIMIT 3")
        print("Query Result (TOON format):")
        print(result)
        print()
    finally:
        adapter.close()


def example_connection_object():
    """Example: Using existing connection object"""
    print("=" * 60)
    print("Example 2: Using Connection Object")
    print("=" * 60)
    
    conn = pymysql.connect(
        host="localhost",
        port=3306,
        user="testuser",
        password="testpass",
        database="testdb"
    )
    adapter = MySQLAdapter(connection=conn)
    
    try:
        result = adapter.query("SELECT * FROM products WHERE price > 100")
        print("Products over $100 (TOON format):")
        print(result)
        print()
    finally:
        adapter.close()  # Note: adapter doesn't own connection, so this won't close it
        conn.close()  # Close manually


def example_connection_params():
    """Example: Using individual connection parameters"""
    print("=" * 60)
    print("Example 3: Using Connection Parameters")
    print("=" * 60)
    
    adapter = MySQLAdapter(
        host="localhost",
        port=3306,
        user="testuser",
        password="testpass",
        database="testdb"
    )
    
    try:
        result = adapter.query("SELECT COUNT(*) as total_users FROM users")
        print("Total users count (TOON format):")
        print(result)
        print()
    finally:
        adapter.close()


def example_select_queries():
    """Example: Various SELECT queries"""
    print("=" * 60)
    print("Example 4: SELECT Queries")
    print("=" * 60)
    
    adapter = MySQLAdapter(connection_string=MYSQL_CONN_STRING)
    
    try:
        # Query with WHERE clause
        print("Users with age > 30:")
        result = adapter.query("SELECT name, age, role FROM users WHERE age > 30")
        print(result)
        print()
        
        # Query with JOIN
        print("Orders with user and product names:")
        result = adapter.query("""
            SELECT 
                u.name as user_name,
                p.name as product_name,
                o.quantity,
                o.total_price,
                o.status
            FROM orders o
            JOIN users u ON o.user_id = u.id
            JOIN products p ON o.product_id = p.id
            LIMIT 3
        """)
        print(result)
        print()
        
        # Query with aggregation
        print("Product categories and counts:")
        result = adapter.query("""
            SELECT category, COUNT(*) as count, AVG(price) as avg_price
            FROM products
            GROUP BY category
        """)
        print(result)
        print()
    finally:
        adapter.close()


def example_non_select_queries():
    """Example: Non-SELECT queries (INSERT, UPDATE, DELETE)"""
    print("=" * 60)
    print("Example 5: Non-SELECT Queries")
    print("=" * 60)
    
    adapter = MySQLAdapter(connection_string=MYSQL_CONN_STRING)
    
    try:
        # INSERT query
        print("Inserting a new user...")
        result = adapter.query("""
            INSERT INTO users (name, email, age, role)
            VALUES ('Test User', 'test@example.com', 27, 'user')
        """)
        print(f"INSERT result (empty TOON): {result}")
        print()
        
        # UPDATE query
        print("Updating user age...")
        result = adapter.query("""
            UPDATE users
            SET age = 28
            WHERE email = 'test@example.com'
        """)
        print(f"UPDATE result (empty TOON): {result}")
        print()
        
        # Verify the update
        print("Verifying update:")
        result = adapter.query("SELECT name, email, age FROM users WHERE email = 'test@example.com'")
        print(result)
        print()
        
        # DELETE query
        print("Deleting test user...")
        result = adapter.query("DELETE FROM users WHERE email = 'test@example.com'")
        print(f"DELETE result (empty TOON): {result}")
        print()
    finally:
        adapter.close()


def example_schema_discovery():
    """Example: Schema discovery"""
    print("=" * 60)
    print("Example 6: Schema Discovery")
    print("=" * 60)
    
    adapter = MySQLAdapter(connection_string=MYSQL_CONN_STRING)
    
    try:
        # Get all tables
        print("All tables in database:")
        tables = adapter.get_tables()
        print(tables)
        print()
        
        # Get schema for specific table
        print("Schema for 'users' table:")
        schema = adapter.get_schema("users")
        print(schema)
        print()
        
        # Get schema for all tables
        print("Schema for all tables:")
        all_schemas = adapter.get_schema()
        for table_name, table_info in all_schemas.items():
            print(f"\n{table_name}:")
            for col in table_info['columns']:
                print(f"  - {col['column_name']}: {col['data_type']} "
                      f"(nullable: {col['is_nullable']})")
        print()
    finally:
        adapter.close()


def example_error_handling():
    """Example: Error handling"""
    print("=" * 60)
    print("Example 7: Error Handling")
    print("=" * 60)
    
    # Connection error
    print("1. Testing connection error:")
    try:
        adapter = MySQLAdapter(
            connection_string="mysql://invalid:invalid@localhost:3306/invalid"
        )
    except ConnectionError as e:
        print(f"   Caught ConnectionError: {e}")
    print()
    
    # Query error
    print("2. Testing query error:")
    adapter = MySQLAdapter(connection_string=MYSQL_CONN_STRING)
    try:
        adapter.query("SELECT * FROM non_existent_table")
    except QueryError as e:
        print(f"   Caught QueryError: {e}")
    finally:
        adapter.close()
    print()
    
    # Schema error
    print("3. Testing schema error:")
    adapter = MySQLAdapter(connection_string=MYSQL_CONN_STRING)
    try:
        adapter.get_schema("non_existent_table")
    except SchemaError as e:
        print(f"   Caught SchemaError: {e}")
    finally:
        adapter.close()
    print()


def example_empty_results():
    """Example: Handling empty result sets"""
    print("=" * 60)
    print("Example 8: Empty Result Sets")
    print("=" * 60)
    
    adapter = MySQLAdapter(connection_string=MYSQL_CONN_STRING)
    
    try:
        # Query that returns no rows
        result = adapter.query("SELECT * FROM users WHERE age > 200")
        print("Query with no results (TOON format):")
        print(result)
        print()
    finally:
        adapter.close()


if __name__ == "__main__":
    print("\nMySQL Adapter Examples")
    print("=" * 60)
    print()
    
    try:
        example_connection_string()
        example_connection_object()
        example_connection_params()
        example_select_queries()
        example_non_select_queries()
        example_schema_discovery()
        example_error_handling()
        example_empty_results()
        
        print("=" * 60)
        print("All examples completed successfully!")
        print("=" * 60)
    except Exception as e:
        print(f"Error running examples: {e}")
        import traceback
        traceback.print_exc()

