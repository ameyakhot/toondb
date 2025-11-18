"""
Setup test databases for PostgreSQL and MySQL
Creates sample tables and inserts test data
"""
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pymysql
import sys

# Connection settings
POSTGRES_CONFIG = {
    "host": "127.0.0.1",
    "port": 5433,
    "user": "testuser",
    "password": "testpass",
    "database": "testdb"
}

MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3307,
    "user": "testuser",
    "password": "testpass",
    "database": "testdb"
}


def setup_postgres():
    """Create PostgreSQL tables and insert test data"""
    print("Setting up PostgreSQL...")
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Create users table
        cursor.execute("""
            DROP TABLE IF EXISTS users CASCADE;
            CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(100) UNIQUE NOT NULL,
                age INTEGER,
                role VARCHAR(50) DEFAULT 'user',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE
            );
        """)
        
        # Create products table
        cursor.execute("""
            DROP TABLE IF EXISTS products CASCADE;
            CREATE TABLE products (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                price DECIMAL(10, 2) NOT NULL,
                category VARCHAR(50),
                in_stock BOOLEAN DEFAULT TRUE,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Create orders table (with foreign key)
        cursor.execute("""
            DROP TABLE IF EXISTS orders CASCADE;
            CREATE TABLE orders (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id),
                product_id INTEGER REFERENCES products(id),
                quantity INTEGER DEFAULT 1,
                total_price DECIMAL(10, 2),
                order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(20) DEFAULT 'pending'
            );
        """)
        
        # Insert users
        cursor.execute("""
            INSERT INTO users (name, email, age, role) VALUES
            ('Alice Johnson', 'alice@example.com', 30, 'admin'),
            ('Bob Smith', 'bob@example.com', 25, 'user'),
            ('Charlie Brown', 'charlie@example.com', 35, 'user'),
            ('Diana Prince', 'diana@example.com', 28, 'moderator'),
            ('Eve Wilson', 'eve@example.com', 32, 'user')
            ON CONFLICT (email) DO NOTHING;
        """)
        
        # Insert products
        cursor.execute("""
            INSERT INTO products (name, price, category, in_stock, description) VALUES
            ('Laptop Pro', 999.99, 'Electronics', TRUE, 'High-performance laptop for professionals'),
            ('Wireless Mouse', 29.99, 'Electronics', TRUE, 'Ergonomic wireless mouse'),
            ('Office Desk', 199.99, 'Furniture', TRUE, 'Modern office desk with storage'),
            ('Gaming Chair', 299.99, 'Furniture', TRUE, 'Comfortable gaming chair with lumbar support'),
            ('USB-C Cable', 19.99, 'Electronics', FALSE, 'Fast charging USB-C cable')
            ON CONFLICT DO NOTHING;
        """)
        
        # Insert orders
        cursor.execute("""
            INSERT INTO orders (user_id, product_id, quantity, total_price, status) VALUES
            (1, 1, 1, 999.99, 'completed'),
            (2, 2, 2, 59.98, 'pending'),
            (3, 3, 1, 199.99, 'completed'),
            (1, 4, 1, 299.99, 'shipped'),
            (4, 2, 1, 29.99, 'completed')
            ON CONFLICT DO NOTHING;
        """)
        
        cursor.close()
        conn.close()
        print("PostgreSQL setup complete!")
        return True
        
    except Exception as e:
        print(f"Error setting up PostgreSQL: {e}")
        return False


def setup_mysql():
    """Create MySQL tables and insert test data"""
    print("Setting up MySQL...")
    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        
        # Drop tables first
        cursor.execute("DROP TABLE IF EXISTS orders")
        cursor.execute("DROP TABLE IF EXISTS products")
        cursor.execute("DROP TABLE IF EXISTS users")
        
        # Create users table
        cursor.execute("""
            CREATE TABLE users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(100) UNIQUE NOT NULL,
                age INT,
                role VARCHAR(50) DEFAULT 'user',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE
            )
        """)
        
        # Create products table
        cursor.execute("""
            CREATE TABLE products (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                price DECIMAL(10, 2) NOT NULL,
                category VARCHAR(50),
                in_stock BOOLEAN DEFAULT TRUE,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create orders table (with foreign key)
        cursor.execute("""
            CREATE TABLE orders (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT,
                product_id INT,
                quantity INT DEFAULT 1,
                total_price DECIMAL(10, 2),
                order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(20) DEFAULT 'pending',
                FOREIGN KEY (user_id) REFERENCES users(id),
                FOREIGN KEY (product_id) REFERENCES products(id)
            )
        """)
        
        # Insert users
        cursor.execute("""
            INSERT INTO users (name, email, age, role) VALUES
            ('Alice Johnson', 'alice@example.com', 30, 'admin'),
            ('Bob Smith', 'bob@example.com', 25, 'user'),
            ('Charlie Brown', 'charlie@example.com', 35, 'user'),
            ('Diana Prince', 'diana@example.com', 28, 'moderator'),
            ('Eve Wilson', 'eve@example.com', 32, 'user')
        """)
        
        # Insert products
        cursor.execute("""
            INSERT INTO products (name, price, category, in_stock, description) VALUES
            ('Laptop Pro', 999.99, 'Electronics', TRUE, 'High-performance laptop for professionals'),
            ('Wireless Mouse', 29.99, 'Electronics', TRUE, 'Ergonomic wireless mouse'),
            ('Office Desk', 199.99, 'Furniture', TRUE, 'Modern office desk with storage'),
            ('Gaming Chair', 299.99, 'Furniture', TRUE, 'Comfortable gaming chair with lumbar support'),
            ('USB-C Cable', 19.99, 'Electronics', FALSE, 'Fast charging USB-C cable')
        """)
        
        # Insert orders
        cursor.execute("""
            INSERT INTO orders (user_id, product_id, quantity, total_price, status) VALUES
            (1, 1, 1, 999.99, 'completed'),
            (2, 2, 2, 59.98, 'pending'),
            (3, 3, 1, 199.99, 'completed'),
            (1, 4, 1, 299.99, 'shipped'),
            (4, 2, 1, 29.99, 'completed')
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        print("MySQL setup complete!")
        return True
        
    except Exception as e:
        print(f"Error setting up MySQL: {e}")
        return False


def verify_data():
    """Verify that data was inserted correctly"""
    print("\nVerifying data...")
    
    # Check PostgreSQL
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM users")
        user_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM products")
        product_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM orders")
        order_count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        print(f"PostgreSQL: {user_count} users, {product_count} products, {order_count} orders")
    except Exception as e:
        print(f"PostgreSQL verification error: {e}")
    
    # Check MySQL
    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM users")
        user_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM products")
        product_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM orders")
        order_count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        print(f"MySQL: {user_count} users, {product_count} products, {order_count} orders")
    except Exception as e:
        print(f"MySQL verification error: {e}")


if __name__ == "__main__":
    print("Setting up test databases...")
    print("=" * 50)
    
    postgres_ok = setup_postgres()
    mysql_ok = setup_mysql()
    
    if postgres_ok and mysql_ok:
        verify_data()
        print("\n" + "=" * 50)
        print("All databases setup complete!")
        print("\nYou can now test the adapters with:")
        print("  PostgreSQL: postgresql://testuser:testpass@localhost:5432/testdb")
        print("  MySQL: mysql://testuser:testpass@localhost:3306/testdb")
    else:
        print("\nSome errors occurred during setup. Please check the output above.")
        sys.exit(1)

