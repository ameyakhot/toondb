"""
Setup test data for MongoDB
Creates sample collections and inserts test data
"""
from pymongo import MongoClient
from datetime import datetime
import sys

# Connection settings
MONGO_CONFIG = {
    "host": "localhost",
    "port": 27017,
    "database": "testdb"
}

MONGO_CONN_STRING = "mongodb://localhost:27017"


def setup_mongodb():
    """Create MongoDB collections and insert test data"""
    print("Setting up MongoDB...")
    try:
        client = MongoClient(MONGO_CONN_STRING)
        db = client[MONGO_CONFIG["database"]]
        
        # Drop existing collections to start fresh
        print("Dropping existing collections...")
        db.users.drop()
        db.products.drop()
        db.orders.drop()
        
        # Insert users
        print("Inserting users...")
        users = [
            {
                "name": "Alice Johnson",
                "email": "alice@example.com",
                "age": 30,
                "role": "admin",
                "is_active": True,
                "created_at": datetime(2024, 1, 15, 10, 30, 0),
                "tags": ["premium", "vip"],
                "address": {
                    "street": "123 Main St",
                    "city": "New York",
                    "zipcode": "10001"
                }
            },
            {
                "name": "Bob Smith",
                "email": "bob@example.com",
                "age": 25,
                "role": "user",
                "is_active": True,
                "created_at": datetime(2024, 2, 20, 14, 15, 0),
                "tags": ["standard"],
                "address": {
                    "street": "456 Oak Ave",
                    "city": "Los Angeles",
                    "zipcode": "90001"
                }
            },
            {
                "name": "Charlie Brown",
                "email": "charlie@example.com",
                "age": 35,
                "role": "user",
                "is_active": False,
                "created_at": datetime(2024, 3, 10, 9, 0, 0),
                "tags": ["premium"],
                "address": {
                    "street": "789 Pine Rd",
                    "city": "Chicago",
                    "zipcode": "60601"
                }
            },
            {
                "name": "Diana Prince",
                "email": "diana@example.com",
                "age": 28,
                "role": "admin",
                "is_active": True,
                "created_at": datetime(2024, 4, 5, 16, 45, 0),
                "tags": ["premium", "vip", "founder"],
                "address": {
                    "street": "321 Elm St",
                    "city": "Seattle",
                    "zipcode": "98101"
                }
            },
            {
                "name": "Eve Wilson",
                "email": "eve@example.com",
                "age": 22,
                "role": "user",
                "is_active": True,
                "created_at": datetime(2024, 5, 12, 11, 20, 0),
                "tags": [],
                "address": {
                    "street": "654 Maple Dr",
                    "city": "Boston",
                    "zipcode": "02101"
                }
            }
        ]
        db.users.insert_many(users)
        print(f"  Inserted {len(users)} users")
        
        # Insert products
        print("Inserting products...")
        products = [
            {
                "name": "Laptop Pro",
                "category": "electronics",
                "price": 1299.99,
                "stock": 50,
                "rating": 4.5,
                "features": ["16GB RAM", "512GB SSD", "Retina Display"],
                "in_stock": True,
                "created_at": datetime(2024, 1, 1, 0, 0, 0)
            },
            {
                "name": "Wireless Mouse",
                "category": "electronics",
                "price": 29.99,
                "stock": 200,
                "rating": 4.2,
                "features": ["Ergonomic", "Battery Life: 2 years"],
                "in_stock": True,
                "created_at": datetime(2024, 1, 5, 0, 0, 0)
            },
            {
                "name": "Desk Chair",
                "category": "furniture",
                "price": 199.99,
                "stock": 30,
                "rating": 4.7,
                "features": ["Adjustable Height", "Lumbar Support"],
                "in_stock": True,
                "created_at": datetime(2024, 2, 1, 0, 0, 0)
            },
            {
                "name": "USB-C Cable",
                "category": "electronics",
                "price": 12.99,
                "stock": 0,
                "rating": 3.8,
                "features": ["Fast Charging", "6ft Length"],
                "in_stock": False,
                "created_at": datetime(2024, 2, 10, 0, 0, 0)
            },
            {
                "name": "Standing Desk",
                "category": "furniture",
                "price": 499.99,
                "stock": 15,
                "rating": 4.9,
                "features": ["Electric Height Adjustment", "Memory Presets"],
                "in_stock": True,
                "created_at": datetime(2024, 3, 1, 0, 0, 0)
            }
        ]
        db.products.insert_many(products)
        print(f"  Inserted {len(products)} products")
        
        # Insert orders
        print("Inserting orders...")
        orders = [
            {
                "user_email": "alice@example.com",
                "product_name": "Laptop Pro",
                "quantity": 1,
                "total": 1299.99,
                "status": "completed",
                "order_date": datetime(2024, 6, 1, 10, 0, 0),
                "shipping_address": {
                    "street": "123 Main St",
                    "city": "New York",
                    "zipcode": "10001"
                }
            },
            {
                "user_email": "bob@example.com",
                "product_name": "Wireless Mouse",
                "quantity": 2,
                "total": 59.98,
                "status": "completed",
                "order_date": datetime(2024, 6, 5, 14, 30, 0),
                "shipping_address": {
                    "street": "456 Oak Ave",
                    "city": "Los Angeles",
                    "zipcode": "90001"
                }
            },
            {
                "user_email": "alice@example.com",
                "product_name": "Desk Chair",
                "quantity": 1,
                "total": 199.99,
                "status": "pending",
                "order_date": datetime(2024, 6, 10, 9, 15, 0),
                "shipping_address": {
                    "street": "123 Main St",
                    "city": "New York",
                    "zipcode": "10001"
                }
            },
            {
                "user_email": "diana@example.com",
                "product_name": "Standing Desk",
                "quantity": 1,
                "total": 499.99,
                "status": "shipped",
                "order_date": datetime(2024, 6, 12, 11, 0, 0),
                "shipping_address": {
                    "street": "321 Elm St",
                    "city": "Seattle",
                    "zipcode": "98101"
                }
            }
        ]
        db.orders.insert_many(orders)
        print(f"  Inserted {len(orders)} orders")
        
        # Verify data
        print("\nVerifying data...")
        user_count = db.users.count_documents({})
        product_count = db.products.count_documents({})
        order_count = db.orders.count_documents({})
        
        print(f"  Users: {user_count}")
        print(f"  Products: {product_count}")
        print(f"  Orders: {order_count}")
        
        if user_count == len(users) and product_count == len(products) and order_count == len(orders):
            print("\n[SUCCESS] MongoDB setup completed successfully!")
            return True
        else:
            print("\n[ERROR] Data verification failed!")
            return False
            
    except Exception as e:
        print(f"\n[ERROR] Error setting up MongoDB: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if 'client' in locals():
            client.close()


if __name__ == "__main__":
    success = setup_mongodb()
    sys.exit(0 if success else 1)

