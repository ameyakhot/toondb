# TOON Database Adapter

Convert your database queries to TOON (Token-Oriented Object Notation) format for efficient LLM usage. Reduce token costs by 30-50% compared to JSON when sending structured data to Large Language Models.

This library provides adapters for MongoDB, PostgreSQL, and MySQL to convert query results into TOON format, making it easy to send database data to LLMs with minimal token usage.

## Why TOON?

When building AI applications, you often need to send structured data to LLMs as context. Traditional JSON format is verbose - every brace, quote, and repeated key consumes tokens. TOON format uses a compact tabular style that's both token-efficient and LLM-friendly.

This library uses the [python-toon](https://github.com/xaviviro/python-toon) library for TOON encoding and decoding, which implements the TOON (Token-Oriented Object Notation) format specification.

**Example Comparison:**

JSON format:
```json
{
  "users": [
    { "id": 1, "name": "Alice", "role": "admin" },
    { "id": 2, "name": "Bob", "role": "user" }
  ]
}
```

TOON format:
```
users[2]{id,name,role}:
  1,Alice,admin
  2,Bob,user
```

The TOON format uses 30-50% fewer tokens while maintaining the same information.

## Features

- **Multi-Database Support**: Adapters for MongoDB, PostgreSQL, and MySQL
- **Token Efficiency**: 30-50% reduction in token usage compared to JSON
- **Works with Existing Databases**: No migration needed - works with your current databases
- **Schema Discovery**: Auto-discover database schemas and table structures
- **Error Handling**: Comprehensive error handling with custom exceptions
- **Easy Integration**: Simple adapter pattern - connect and query as usual
- **Python-First**: Clean, intuitive API designed for Python developers
- **Bidirectional Conversion**: Encode to TOON, decode back to Python data structures
- **TOON-to-TOON Round-Trips**: Insert/update data from TOON and immediately query it back in the same session

## Installation

Install from PyPI:

```bash
pip install toondb
```

Or using `uv`:

```bash
uv pip install toondb
```

## Quick Start

### Unified Connection (Recommended)

The easiest way to connect to any database is using the unified `connect()` function, which auto-detects the database type:

```python
from toonpy import connect

# PostgreSQL - auto-detected from connection string
adapter = connect("postgresql://user:pass@localhost:5432/mydb")
toon_result = adapter.query("SELECT name, age FROM users WHERE age > 30")
adapter.close()

# MySQL - auto-detected
adapter = connect("mysql://user:pass@localhost:3306/mydb")
toon_result = adapter.query("SELECT name, email FROM users LIMIT 5")
adapter.close()

# MongoDB - requires database and collection_name
adapter = connect("mongodb://localhost:27017", database="mydb", collection_name="users")
toon_result = adapter.find({"age": {"$gt": 30}})
adapter.close()
```

The `connect()` function automatically detects the database type from the connection string prefix:
- `postgresql://` or `postgres://` → PostgreSQL
- `mysql://` or `mysql+pymysql://` → MySQL
- `mongodb://` or `mongodb+srv://` → MongoDB

You can also specify the database type explicitly:

```python
# Explicit type specification
adapter = connect("some://url", db_type="postgresql")

# Individual parameters
adapter = connect(
    db_type="postgresql",
    host="localhost",
    port=5432,
    user="user",
    password="pass",
    database="mydb"
)
```

### PostgreSQL Example

Connect to your PostgreSQL database and convert query results to TOON format:

```python
from toonpy import PostgresAdapter

# Create adapter with connection string
adapter = PostgresAdapter(
    connection_string="postgresql://user:pass@localhost:5432/mydb"
)

# Query and get TOON format (ready for LLM!)
toon_result = adapter.query("SELECT name, age FROM users WHERE age > 30")
print(toon_result)
adapter.close()
```

Output:
```
[2,]{name,age}:
  Alice,35
  Bob,40
```

### MySQL Example

```python
from toonpy import MySQLAdapter

# Create adapter with connection string
adapter = MySQLAdapter(
    connection_string="mysql://user:pass@localhost:3306/mydb"
)

# Query and get TOON format
toon_result = adapter.query("SELECT name, email FROM users LIMIT 5")
print(toon_result)
adapter.close()
```

### MongoDB Example

```python
from toonpy import MongoAdapter
from pymongo import MongoClient

# Connect to your existing MongoDB
client = MongoClient("mongodb://localhost:27017")
collection = client.mydb.users

# Create adapter
adapter = MongoAdapter(collection=collection)

# Query and get TOON format (ready for LLM!)
toon_result = adapter.find({"age": {"$gt": 30}})
print(toon_result)
```

### Using the Converter Directly

You can also convert Python data structures directly:

```python
from toonpy import to_toon, from_toon

# Convert Python data to TOON
data = [
    {"id": 1, "name": "Alice", "role": "admin"},
    {"id": 2, "name": "Bob", "role": "user"}
]
toon_string = to_toon(data)

# Convert back from TOON
decoded_data = from_toon(toon_string)
```

## Usage

### Unified Connection Function

The `connect()` function provides a single interface for connecting to all supported databases:

```python
from toonpy import connect

# Auto-detection from connection string
adapter = connect("postgresql://user:pass@localhost:5432/mydb")
adapter = connect("mysql://user:pass@localhost:3306/mydb")
adapter = connect("mongodb://localhost:27017", database="mydb", collection_name="users")

# Explicit type specification
adapter = connect("custom://url", db_type="postgresql")

# Individual parameters
adapter = connect(
    db_type="postgresql",
    host="localhost",
    port=5432,
    user="user",
    password="pass",
    database="mydb"
)
```

**Error Handling:** If you provide an unrecognized connection string, `connect()` will provide helpful error messages with suggestions. For example:

```python
# SQLite (not supported)
connect("sqlite:///db.db")
# Error: SQLite is not currently supported. Supported databases: PostgreSQL, MySQL, MongoDB

# Missing protocol
connect("localhost:5432/mydb")
# Error: Connection string appears to be missing a protocol prefix...
```

### PostgreSQL Adapter

The `PostgresAdapter` works with your existing PostgreSQL databases:

```python
from toonpy import PostgresAdapter

# Option 1: Use connection string
adapter = PostgresAdapter(
    connection_string="postgresql://user:pass@localhost:5432/mydb"
)

# Option 2: Use individual parameters
adapter = PostgresAdapter(
    host="localhost",
    port=5432,
    user="user",
    password="pass",
    database="mydb"
)

# Option 3: Use existing connection
import psycopg2
conn = psycopg2.connect("postgresql://user:pass@localhost:5432/mydb")
adapter = PostgresAdapter(connection=conn)

# Query methods
toon_result = adapter.query("SELECT * FROM users WHERE age > 30")
toon_result = adapter.execute("SELECT name, email FROM users")  # Alias for query()

# Schema discovery
schema = adapter.get_schema("users")  # Get single table schema
all_schemas = adapter.get_schema()   # Get all tables
tables = adapter.get_tables()        # List all tables

adapter.close()
```

### MySQL Adapter

The `MySQLAdapter` works with your existing MySQL databases:

```python
from toonpy import MySQLAdapter

# Option 1: Use connection string
adapter = MySQLAdapter(
    connection_string="mysql://user:pass@localhost:3306/mydb"
)

# Option 2: Use individual parameters
adapter = MySQLAdapter(
    host="localhost",
    port=3306,
    user="user",
    password="pass",
    database="mydb"
)

# Query methods
toon_result = adapter.query("SELECT * FROM users WHERE age > 30")

# Schema discovery
schema = adapter.get_schema("users")
tables = adapter.get_tables(include_views=True)

adapter.close()
```

### MongoDB Adapter

The `MongoAdapter` works with your existing MongoDB collections:

```python
from toonpy import MongoAdapter
from pymongo import MongoClient

# Option 1: Pass existing collection
client = MongoClient("mongodb://localhost:27017")
collection = client.mydb.users
adapter = MongoAdapter(collection=collection)

# Option 2: Use connection string
adapter = MongoAdapter(
    connection_string="mongodb://localhost:27017",
    database="mydb",
    collection_name="users"
)

# Query methods
toon_result = adapter.find({"role": "admin"})  # MongoDB find query
toon_result = adapter.query({"role": "admin"})  # Same as find (implements abstract method)

# With projection
toon_result = adapter.find(
    {"age": {"$gt": 30}},
    projection={"name": 1, "age": 1, "_id": 0}
)
```

### Advanced Usage

```python
# PostgreSQL: Complex queries with JOINs
adapter = PostgresAdapter(connection_string="postgresql://...")
toon_result = adapter.query("""
    SELECT u.name, p.name as product_name, o.quantity
    FROM orders o
    JOIN users u ON o.user_id = u.id
    JOIN products p ON o.product_id = p.id
    WHERE o.status = 'completed'
""")

# MySQL: Aggregation queries
adapter = MySQLAdapter(connection_string="mysql://...")
toon_result = adapter.query("""
    SELECT category, COUNT(*) as count, AVG(price) as avg_price
    FROM products
    GROUP BY category
""")

# MongoDB: Query with nested fields (using docker test data)
adapter = MongoAdapter(
    connection_string="mongodb://localhost:27017",
    database="testdb",
    collection_name="users"
)
toon_result = adapter.find({
    "address.city": "New York",
    "age": {"$gt": 25}
})
```

## API Reference

### PostgresAdapter

#### `__init__(connection_string=None, connection=None, **kwargs)`

Initialize PostgreSQL adapter.

**Parameters:**
- `connection_string`: PostgreSQL connection string (postgresql://user:pass@host:port/dbname)
- `connection`: Existing psycopg2 connection object (optional)
- `**kwargs`: Individual connection parameters (host, port, user, password, database)

**Example:**
```python
# Using connection string
adapter = PostgresAdapter(connection_string="postgresql://user:pass@localhost:5432/mydb")

# Using individual parameters
adapter = PostgresAdapter(host="localhost", port=5432, user="user", password="pass", database="mydb")

# Using existing connection
import psycopg2
conn = psycopg2.connect("postgresql://...")
adapter = PostgresAdapter(connection=conn)
```

#### `query(sql: str, params: Optional[Union[tuple, dict, list]] = None) -> str`

Execute SQL query and return results in TOON format.

**Parameters:**
- `sql`: SQL query string (use `%s` placeholders for parameters)
- `params`: Optional parameters for parameterized query (tuple, dict, or list). When provided, prevents SQL injection by using database parameterization.

**Returns:** TOON formatted string

**Security Note:** Always use parameterized queries when including user input to prevent SQL injection attacks.

**Examples:**
```python
# Safe: Parameterized query (recommended)
toon_result = adapter.query("SELECT name, age FROM users WHERE id = %s", (123,))
toon_result = adapter.query("SELECT * FROM users WHERE name = %s AND age > %s", ("Alice", 30))

# Safe: Parameterized query with dict parameters
toon_result = adapter.query(
    "SELECT * FROM users WHERE id = %(user_id)s AND age > %(min_age)s",
    {"user_id": 123, "min_age": 20}
)

# Use with caution: Raw SQL (only for trusted, static queries)
toon_result = adapter.query("SELECT name, age FROM users WHERE age > 30")

# Unsafe: String concatenation (SQL injection risk)
user_id = "123; DROP TABLE users; --"
toon_result = adapter.query(f"SELECT * FROM users WHERE id = {user_id}")  # DON'T DO THIS
```

#### `execute(sql: str, params: Optional[Union[tuple, dict, list]] = None) -> str`

Alias for `query()` method. Supports the same parameterized query syntax.

#### `get_schema(table=None, schema='public') -> Dict`

Get database schema information.

**Parameters:**
- `table`: Table name (optional, if None returns all tables)
- `schema`: Schema name (default: 'public')

**Returns:** Dictionary with schema information

#### `get_tables(include_views=False, schema='public') -> List[str]`

List all tables in the database.

**Parameters:**
- `include_views`: If True, include views (default: False)
- `schema`: Schema name (default: 'public')

**Returns:** List of table names

#### `close()`

Close PostgreSQL connection if adapter owns it.

#### `insert_and_query_from_toon(table: str, toon_string: str, where: Optional[Dict] = None, schema: str = 'public', projection: Optional[List[str]] = None, on_conflict: Optional[str] = None) -> str`

Insert single row from TOON and immediately query it back as TOON. Uses the same instance/session - guaranteed to work.

**Parameters:**
- `table`: Table name
- `toon_string`: TOON formatted string containing row data
- `where`: Optional WHERE clause dict to query back inserted row. If None, uses RETURNING clause to get inserted ID or all inserted values
- `schema`: Schema name (default: 'public')
- `projection`: Optional list of column names to select (defaults to all columns)
- `on_conflict`: PostgreSQL ON CONFLICT clause

**Returns:** TOON formatted string with queried row data

**Example:**
```python
from toonpy import PostgresAdapter, to_toon

adapter = PostgresAdapter(connection_string="postgresql://...")
document = {"name": "Alice", "age": 30}
toon_string = to_toon([document])
result = adapter.insert_and_query_from_toon("users", toon_string)
# Returns TOON with the inserted row (with id if exists)
```

#### `insert_many_and_query_from_toon(table: str, toon_string: str, where: Optional[Dict] = None, schema: str = 'public', projection: Optional[List[str]] = None, limit: Optional[int] = None) -> str`

Insert multiple rows from TOON and immediately query them back as TOON. Uses the same instance/session - guaranteed to work.

**Parameters:**
- `table`: Table name
- `toon_string`: TOON formatted string containing list of rows
- `where`: Optional WHERE clause dict to query back inserted rows. If None, queries by all inserted column values using IN operator
- `schema`: Schema name (default: 'public')
- `projection`: Optional list of column names to select (defaults to all columns)
- `limit`: Optional limit on number of rows to return

**Returns:** TOON formatted string with queried rows

**Example:**
```python
documents = [{"name": "Alice"}, {"name": "Bob"}]
toon_string = to_toon(documents)
result = adapter.insert_many_and_query_from_toon("users", toon_string)
# Returns TOON with both inserted rows
```

#### `update_and_query_from_toon(table: str, toon_string: str, where: Dict[str, Any], schema: str = 'public', projection: Optional[List[str]] = None) -> str`

Update rows from TOON and immediately query them back as TOON. Uses the same instance/session - guaranteed to work.

**Parameters:**
- `table`: Table name
- `toon_string`: TOON formatted string with update data
- `where`: WHERE clause conditions as dict to find rows to update
- `schema`: Schema name (default: 'public')
- `projection`: Optional list of column names to select (defaults to all columns)

**Returns:** TOON formatted string with updated row data

**Example:**
```python
update_data = {"age": 31, "status": "active"}
toon_string = to_toon([update_data])
result = adapter.update_and_query_from_toon("users", toon_string, where={"id": 123})
# Returns TOON with updated row
```

### MySQLAdapter

#### `__init__(connection_string=None, connection=None, **kwargs)`

Initialize MySQL adapter.

**Parameters:**
- `connection_string`: MySQL connection string (mysql://user:pass@host:port/dbname)
- `connection`: Existing pymysql connection object (optional)
- `**kwargs`: Individual connection parameters (host, port, user, password, database)

**Example:**
```python
# Using connection string
adapter = MySQLAdapter(connection_string="mysql://user:pass@localhost:3306/mydb")

# Using individual parameters
adapter = MySQLAdapter(host="localhost", port=3306, user="user", password="pass", database="mydb")
```

#### `query(sql: str, params: Optional[Union[tuple, dict, list]] = None) -> str`

Execute SQL query and return results in TOON format.

**Parameters:**
- `sql`: SQL query string (use `%s` placeholders for parameters)
- `params`: Optional parameters for parameterized query (tuple, dict, or list). When provided, prevents SQL injection by using database parameterization.

**Returns:** TOON formatted string

**Security Note:** Always use parameterized queries when including user input to prevent SQL injection attacks.

**Examples:**
```python
# Safe: Parameterized query (recommended)
toon_result = adapter.query("SELECT name, email FROM users WHERE id = %s", (123,))
toon_result = adapter.query("SELECT * FROM users WHERE name = %s AND age > %s", ("Alice", 30))

# Safe: Parameterized query with dict parameters
toon_result = adapter.query(
    "SELECT * FROM users WHERE id = %(user_id)s AND age > %(min_age)s",
    {"user_id": 123, "min_age": 20}
)

# Use with caution: Raw SQL (only for trusted, static queries)
toon_result = adapter.query("SELECT name, email FROM users LIMIT 5")
```

#### `execute(sql: str, params: Optional[Union[tuple, dict, list]] = None) -> str`

Alias for `query()` method. Supports the same parameterized query syntax.

#### `get_schema(table=None, database=None) -> Dict`

Get database schema information.

**Parameters:**
- `table`: Table name (optional, if None returns all tables)
- `database`: Database name (optional, defaults to current database)

**Returns:** Dictionary with schema information

#### `get_tables(include_views=False, database=None) -> List[str]`

List all tables in the database.

**Parameters:**
- `include_views`: If True, include views (default: False)
- `database`: Database name (optional, defaults to current database)

**Returns:** List of table names

#### `close()`

Close MySQL connection if adapter owns it.

#### `insert_and_query_from_toon(table: str, toon_string: str, where: Optional[Dict] = None, database: Optional[str] = None, projection: Optional[List[str]] = None, on_duplicate_key_update: Optional[str] = None) -> str`

Insert single row from TOON and immediately query it back as TOON. Uses the same instance/session - guaranteed to work.

**Parameters:**
- `table`: Table name
- `toon_string`: TOON formatted string containing row data
- `where`: Optional WHERE clause dict to query back inserted row. If None, uses LAST_INSERT_ID() for auto-increment keys or all inserted values
- `database`: Database name (optional, defaults to current database)
- `projection`: Optional list of column names to select (defaults to all columns)
- `on_duplicate_key_update`: MySQL ON DUPLICATE KEY UPDATE clause

**Returns:** TOON formatted string with queried row data

**Example:**
```python
from toonpy import MySQLAdapter, to_toon

adapter = MySQLAdapter(connection_string="mysql://...")
document = {"name": "Alice", "age": 30}
toon_string = to_toon([document])
result = adapter.insert_and_query_from_toon("users", toon_string)
# Returns TOON with the inserted row (with auto-increment id if exists)
```

#### `insert_many_and_query_from_toon(table: str, toon_string: str, where: Optional[Dict] = None, database: Optional[str] = None, projection: Optional[List[str]] = None, limit: Optional[int] = None) -> str`

Insert multiple rows from TOON and immediately query them back as TOON. Uses the same instance/session - guaranteed to work.

**Parameters:**
- `table`: Table name
- `toon_string`: TOON formatted string containing list of rows
- `where`: Optional WHERE clause dict to query back inserted rows. If None, queries by all inserted column values using IN operator
- `database`: Database name (optional, defaults to current database)
- `projection`: Optional list of column names to select (defaults to all columns)
- `limit`: Optional limit on number of rows to return

**Returns:** TOON formatted string with queried rows

**Example:**
```python
documents = [{"name": "Alice"}, {"name": "Bob"}]
toon_string = to_toon(documents)
result = adapter.insert_many_and_query_from_toon("users", toon_string)
# Returns TOON with both inserted rows
```

#### `update_and_query_from_toon(table: str, toon_string: str, where: Dict[str, Any], database: Optional[str] = None, projection: Optional[List[str]] = None) -> str`

Update rows from TOON and immediately query them back as TOON. Uses the same instance/session - guaranteed to work.

**Parameters:**
- `table`: Table name
- `toon_string`: TOON formatted string with update data
- `where`: WHERE clause conditions as dict to find rows to update
- `database`: Database name (optional, defaults to current database)
- `projection`: Optional list of column names to select (defaults to all columns)

**Returns:** TOON formatted string with updated row data

**Example:**
```python
update_data = {"age": 31, "status": "active"}
toon_string = to_toon([update_data])
result = adapter.update_and_query_from_toon("users", toon_string, where={"id": 123})
# Returns TOON with updated row
```

### MongoAdapter

#### `__init__(collection=None, connection_string=None, database=None, collection_name=None)`

Initialize MongoDB adapter.

**Parameters:**
- `collection`: Existing MongoDB collection object (optional)
- `connection_string`: MongoDB connection string (optional, requires database and collection_name)
- `database`: Database name (required if using connection_string)
- `collection_name`: Collection name (required if using connection_string)

**Example:**
```python
# Using existing collection
adapter = MongoAdapter(collection=my_collection)

# Using connection string
adapter = MongoAdapter(
    connection_string="mongodb://localhost:27017",
    database="mydb",
    collection_name="users"
)
```

#### `find(query=None, projection=None) -> str`

Execute MongoDB find query and return results in TOON format.

**Parameters:**
- `query`: MongoDB query dictionary (default: `{}`)
- `projection`: MongoDB projection dictionary (optional)

**Returns:** TOON formatted string

**Example:**
```python
toon_result = adapter.find({"age": {"$gt": 30}})
```

#### `query(query=None) -> str`

Execute query (implements abstract method from BaseAdapter). Accepts either a JSON string or a dictionary.

**Parameters:**
- `query`: MongoDB query as JSON string or dictionary (optional)

**Returns:** TOON formatted string

#### `close()`

Close MongoDB connection if adapter owns it.

### Converter Functions

The converter functions use the [python-toon](https://github.com/xaviviro/python-toon) library under the hood for encoding and decoding.

#### `to_toon(data: List[Dict]) -> str`

Convert Python data structures to TOON format using the python-toon library.

**Parameters:**
- `data`: List of dictionaries (query results)

**Returns:** TOON formatted string

**Example:**
```python
data = [{"id": 1, "name": "Alice"}]
toon_string = to_toon(data)
```

#### `from_toon(toon_string: str) -> List[Dict]`

Convert TOON format back to Python data structure using the python-toon library.

**Parameters:**
- `toon_string`: TOON formatted string

**Returns:** List of dictionaries

**Example:**
```python
decoded_data = from_toon(toon_string)
```

## Supported Databases

- **PostgreSQL** - Fully supported with `PostgresAdapter`
  - Schema discovery
  - All SQL query types (SELECT, INSERT, UPDATE, DELETE)
  - Comprehensive data type support (arrays, JSON, UUID, etc.)

- **MySQL** - Fully supported with `MySQLAdapter`
  - Schema discovery
  - All SQL query types (SELECT, INSERT, UPDATE, DELETE)
  - MySQL-specific types (ENUM, SET, JSON)

- **MongoDB** - Fully supported with `MongoAdapter`
  - MongoDB query syntax
  - Projection support
  - Nested document queries

## Use Cases

- **RAG Applications**: Send database results to LLMs as context
- **AI Chatbots**: Include product catalogs, user data, etc. efficiently
- **Data Analysis**: Pass structured data to AI models with reduced token costs
- **Fine-tuning**: Prepare training data in token-efficient format
- **API Responses**: Return data in TOON format for LLM consumption

## Security Best Practices

### SQL Injection Prevention

**Always use parameterized queries when including user input:**

```python
# Safe: Parameterized query
adapter.query("SELECT * FROM users WHERE id = %s", (user_id,))
adapter.query("SELECT * FROM users WHERE name = %s AND age > %s", (name, min_age))

# Unsafe: String concatenation (SQL injection risk)
adapter.query(f"SELECT * FROM users WHERE id = {user_id}")  # DON'T DO THIS
adapter.query("SELECT * FROM users WHERE id = " + str(user_id))  # DON'T DO THIS
```

### Key Security Points

1. **Use Parameterized Queries**: The `params` argument prevents SQL injection by using database-level parameterization
2. **Never Concatenate User Input**: Always use the `params` argument for any user-provided values
3. **Validate Input**: Validate and sanitize user input before passing to queries
4. **Principle of Least Privilege**: Use database users with minimal required permissions
5. **Connection Security**: Use encrypted connections (SSL/TLS) in production

### Example: Secure User Lookup

```python
from toonpy import connect

# Connect to database
adapter = connect("postgresql://user:pass@localhost:5432/mydb")

# Safe: Parameterized query
def get_user(user_id: int):
    return adapter.query("SELECT * FROM users WHERE id = %s", (user_id,))

# Unsafe: String formatting
def get_user_unsafe(user_id: int):
    return adapter.query(f"SELECT * FROM users WHERE id = {user_id}")  # VULNERABLE
```

## Requirements

- Python 3.8+
- [python-toon](https://github.com/xaviviro/python-toon) >= 1.0.0 - TOON format encoder/decoder

**Database-specific dependencies:**
- `psycopg2-binary` (for PostgreSQL support)
- `pymysql` (for MySQL support)
- `pymongo` (for MongoDB support)

Install all dependencies:
```bash
pip install toondb psycopg2-binary pymysql pymongo
```

Or install only what you need:
```bash
# PostgreSQL only
pip install toondb psycopg2-binary

# MySQL only
pip install toondb pymysql

# MongoDB only
pip install toondb pymongo
```

## Development

### Setup Development Environment

```bash
# Clone the repository
git clone https://github.com/ameyakhot/toondb.git
cd toondb

# Install in development mode
pip install -e .

# Install development dependencies
pip install -r requirements.txt
```

### Running Tests

```bash
# Run tests (when test suite is available)
pytest
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Credits

This library is built on top of the [python-toon](https://github.com/xaviviro/python-toon) library by [xaviviro](https://github.com/xaviviro), which provides the core TOON encoding and decoding functionality. The python-toon library implements the TOON (Token-Oriented Object Notation) format specification and is licensed under the MIT License.

## License

MIT License

## Links

- PyPI: https://pypi.org/project/toondb/
- GitHub: https://github.com/ameyakhot/toondb
- Issues: https://github.com/ameyakhot/toondb/issues
- python-toon library: https://github.com/xaviviro/python-toon

## Changelog

### 0.1.5 (Current)

- **TOON-to-TOON Round-Trip Methods** - New methods for MySQL and PostgreSQL adapters that ensure session continuity
  - `insert_and_query_from_toon()` - Insert and immediately query back in same session
  - `insert_many_and_query_from_toon()` - Bulk insert and query back in same session
  - `update_and_query_from_toon()` - Update and query back in same session
- **Session Continuity** - All round-trip methods use the same adapter instance, solving the "session is over" problem
- **Smart Primary Key Detection** - MySQL uses LAST_INSERT_ID() and PostgreSQL uses RETURNING clause when available
- **Comprehensive Testing** - Added unit and integration tests for all new methods

### 0.1.4

- **PostgreSQL adapter** - Full support with schema discovery
- **MySQL adapter** - Full support with schema discovery
- **Error handling** - Custom exception classes (ConnectionError, QueryError, SchemaError)
- **Schema discovery** - Auto-discover database schemas for PostgreSQL and MySQL
- **Comprehensive type support** - Handle arrays, JSON, UUID, BLOB, and more
- **Transaction management** - Automatic rollback on errors
- **Enhanced documentation** - Examples for all three databases

### 0.1.0 (Initial Release)

- MongoDB adapter support
- TOON converter functions (to_toon, from_toon)
- Basic query support
- Connection management

