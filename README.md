# TOON Database Adapter

Convert your database queries to TOON (Token-Oriented Object Notation) format for efficient LLM usage. Reduce token costs by 30-50% compared to JSON when sending structured data to Large Language Models.

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

- **Token Efficiency**: 30-50% reduction in token usage compared to JSON
- **Works with Existing Databases**: No migration needed - works with your current MongoDB, PostgreSQL, and more
- **Easy Integration**: Simple adapter pattern - connect and query as usual
- **Python-First**: Clean, intuitive API designed for Python developers
- **Bidirectional Conversion**: Encode to TOON, decode back to Python data structures

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

### MongoDB Example

```python
from toondb import MongoAdapter
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

Output:
```
users[2]{id,name,age}:
  1,Alice,35
  2,Bob,40
```

### Using the Converter Directly

You can also convert Python data structures directly:

```python
from toondb import to_toon, from_toon

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

### MongoDB Adapter

The `MongoAdapter` works with your existing MongoDB collections:

```python
from toondb import MongoAdapter
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
# Query with limit (using pymongo cursor)
results_cursor = collection.find({"category": "electronics"}).limit(10)
results = list(results_cursor)
toon_result = adapter._to_toon(results)

# Nested queries
toon_result = adapter.find({
    "categories_v2.main_category": "Eat",
    "city": "Mumbai"
})
```

## API Reference

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

- MongoDB (fully supported)
- PostgreSQL (coming soon)
- MySQL (coming soon)

## Use Cases

- **RAG Applications**: Send database results to LLMs as context
- **AI Chatbots**: Include product catalogs, user data, etc. efficiently
- **Data Analysis**: Pass structured data to AI models with reduced token costs
- **Fine-tuning**: Prepare training data in token-efficient format
- **API Responses**: Return data in TOON format for LLM consumption

## Requirements

- Python 3.8+
- [python-toon](https://github.com/xaviviro/python-toon) >= 0.1.3 - TOON format encoder/decoder
- pymongo (for MongoDB support)
- psycopg2-binary (for PostgreSQL support, coming soon)

## Development

### Setup Development Environment

```bash
# Clone the repository
git clone https://github.com/yourusername/toondb.git
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
- GitHub: https://github.com/yourusername/toondb
- Issues: https://github.com/yourusername/toondb/issues
- python-toon library: https://github.com/xaviviro/python-toon

## Changelog

### 0.1.0 (Initial Release)

- MongoDB adapter support
- TOON converter functions (to_toon, from_toon)
- Basic query support
- Connection management

