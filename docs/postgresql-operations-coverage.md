# PostgreSQL Operations Coverage & Implementation Plan

## Overview

This document outlines the complete PostgreSQL operations coverage for the `toondb` package, focusing on bidirectional conversion between SQL (PostgreSQL native format) and TOON (Token-Oriented Object Notation) format.

## Current Coverage Analysis

### ✅ Currently Implemented Operations

| Operation | Direction | Status | Method | Notes |
|-----------|-----------|--------|--------|-------|
| `query()` | SQL → TOON | ✅ Implemented | `PostgresAdapter.query()` | Returns TOON string |
| `execute()` | SQL → TOON | ✅ Implemented | `PostgresAdapter.execute()` | Alias for query() |
| `get_schema()` | SQL → TOON | ✅ Implemented | `PostgresAdapter.get_schema()` | Schema discovery |
| `get_tables()` | SQL → TOON | ✅ Implemented | `PostgresAdapter.get_tables()` | Table listing |
| `insert_one_from_toon()` | TOON → SQL → PostgreSQL | ✅ Implemented | `PostgresAdapter.insert_one_from_toon()` | Insert single row |
| `insert_many_from_toon()` | TOON → SQL → PostgreSQL | ✅ Implemented | `PostgresAdapter.insert_many_from_toon()` | Insert multiple rows |
| `update_from_toon()` | TOON → SQL → PostgreSQL | ✅ Implemented | `PostgresAdapter.update_from_toon()` | Update rows |
| `delete_from_toon()` | JSON → SQL → PostgreSQL | ✅ Implemented | `PostgresAdapter.delete_from_toon()` | Delete rows |

**Current Coverage: 100% (8/8 core operations) - Phase 1 Complete!**

### Current Implementation Flow

```
SQL Query (string with optional params)
  → PostgresAdapter.query()
  → PostgreSQL cursor.execute()
  → List[Dict] results
  → _clean_postgres_data() (Decimal → float, datetime → isoformat, UUID → str, bytes → base64)
  → _to_toon() → TOON string
```

## Bidirectional Conversion Matrix

### Current State

| Conversion | Status | Implementation |
|------------|--------|----------------|
| SQL Query → TOON Results | ✅ Done | `query()`, `execute()`, `get_schema()`, `get_tables()` |
| TOON → SQL (for inserts) | ✅ Done | `insert_one_from_toon()`, `insert_many_from_toon()` |
| TOON → SQL (for updates) | ✅ Done | `update_from_toon()` |
| TOON → SQL (for deletes) | ✅ Done | `delete_from_toon()` |
| TOON → Python Dict | ✅ Done | `from_toon()` (direct) |
| Python Dict → TOON | ✅ Done | `to_toon()` (direct) |

### Complete Bidirectional Coverage

```
┌─────────────────────────────────────────────────────────────┐
│                  PostgreSQL Operations                       │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  READ OPERATIONS (SQL → TOON)                                 │
│  ✅ query()                                                    │
│  ✅ execute()                                                  │
│  ✅ get_schema()                                               │
│  ✅ get_tables()                                               │
│                                                               │
│  WRITE OPERATIONS (TOON → SQL → PostgreSQL)                  │
│  ✅ insert_one_from_toon()                                    │
│  ✅ insert_many_from_toon()                                   │
│  ✅ update_from_toon()                                        │
│  ✅ delete_from_toon()                                        │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

## Implementation Details

### SQL Generation Strategy

All SQL generation uses **parameterized queries** to prevent SQL injection:

1. **Table Name Validation**: Regex validation to ensure only safe characters
2. **Column Validation**: Schema lookup to verify columns exist
3. **Parameterization**: All values use `%s` placeholders
4. **Type Conversion**: Automatic conversion from TOON types to PostgreSQL types

### Security Features

#### Table Name Validation
```python
def _validate_table_name(self, table: str) -> None:
    """Validates table name using regex to prevent SQL injection"""
    # Only allows: alphanumeric, underscore, dot (for schema.table)
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', table):
        raise SecurityError("Invalid table name")
```

#### Column Validation
```python
def _validate_column_names(self, table: str, columns: List[str], schema: str = 'public') -> None:
    """Validates columns exist in table schema"""
    # Queries information_schema to verify columns
    # Raises SchemaError if any column doesn't exist
```

#### Parameterized Queries
```python
# ✅ SAFE: Parameterized
sql = "INSERT INTO users (name, age) VALUES (%s, %s)"
params = ["Alice", 30]
cursor.execute(sql, params)

# ❌ UNSAFE: String formatting (NEVER USED)
sql = f"INSERT INTO users (name, age) VALUES ('{name}', {age})"  # NEVER
```

### Type Conversion: TOON → PostgreSQL

When writing TOON data back to PostgreSQL, automatic type conversion handles:

| TOON Type | PostgreSQL Type | Conversion |
|-----------|-----------------|------------|
| ISO date string | `date` | `date.fromisoformat()` |
| ISO datetime string | `timestamp` | `datetime.fromisoformat()` |
| UUID string | `uuid` | `uuid.UUID()` |
| Base64 string | `bytea` | `base64.b64decode()` |
| List | Array | Recursive conversion |
| Dict | JSON/JSONB | Recursive conversion |
| Primitives | Primitives | Pass-through |

### SQL Generation Methods

#### INSERT SQL Generation
```python
def _generate_insert_sql(
    self, 
    table: str, 
    data: Union[Dict, List[Dict]], 
    schema: str = 'public',
    on_conflict: Optional[str] = None
) -> Tuple[str, List[Any]]:
    """
    Generates parameterized INSERT SQL.
    
    Single row: INSERT INTO users (name, age) VALUES (%s, %s)
    Multiple rows: INSERT INTO users (name, age) VALUES (%s, %s), (%s, %s)
    """
```

#### UPDATE SQL Generation
```python
def _generate_update_sql(
    self,
    table: str,
    data: Dict,
    where: Dict[str, Any],
    schema: str = 'public'
) -> Tuple[str, List[Any]]:
    """
    Generates parameterized UPDATE SQL.
    
    Example: UPDATE users SET age = %s WHERE id = %s
    """
```

#### DELETE SQL Generation
```python
def _generate_delete_sql(
    self,
    table: str,
    where: Dict[str, Any],
    schema: str = 'public'
) -> Tuple[str, List[Any]]:
    """
    Generates parameterized DELETE SQL.
    
    Example: DELETE FROM users WHERE status = %s
    """
```

## Implementation Flow Examples

### Example 1: Insert Flow

```
User has TOON data:
  users[1]{name,age,email}:
    Alice,30,alice@example.com

Flow:
  1. insert_one_from_toon("users", toon_string)
  2. from_toon(toon_string) → Dict
     {"name": "Alice", "age": 30, "email": "alice@example.com"}
  3. get_schema("users") → Column types for conversion
  4. _convert_to_postgres_value() → Convert types if needed
  5. _generate_insert_sql("users", data) → 
     ("INSERT INTO users (name, age, email) VALUES (%s, %s, %s)", ["Alice", 30, "alice@example.com"])
  6. cursor.execute(sql, params)
  7. Result: rowcount = 1
  8. Convert result to TOON: to_toon([{"rowcount": 1}])
  9. Return TOON string
```

### Example 2: Update Flow

```
User has TOON update data:
  update[1]{age,status}:
    31,active

Flow:
  1. update_from_toon("users", toon_string, where={"id": 123})
  2. from_toon(toon_string) → Dict
     {"age": 31, "status": "active"}
  3. get_schema("users") → Column types
  4. _convert_to_postgres_value() → Convert types
  5. _generate_update_sql("users", data, where) →
     ("UPDATE users SET age = %s, status = %s WHERE id = %s", [31, "active", 123])
  6. cursor.execute(sql, params)
  7. Result: rowcount = 1
  8. Convert result to TOON: to_toon([{"rowcount": 1}])
  9. Return TOON string
```

### Example 3: Delete Flow

```
User wants to delete:
  WHERE email = "test@example.com"

Flow:
  1. delete_from_toon("users", where={"email": "test@example.com"})
  2. get_schema("users") → Column types
  3. _convert_to_postgres_value() → Convert types
  4. _generate_delete_sql("users", where) →
     ("DELETE FROM users WHERE email = %s", ["test@example.com"])
  5. cursor.execute(sql, params)
  6. Result: rowcount = 1
  7. Convert result to TOON: to_toon([{"rowcount": 1}])
  8. Return TOON string
```

## Method Signatures

### Insert Operations

```python
def insert_one_from_toon(
    self, 
    table: str, 
    toon_string: str, 
    schema: str = 'public',
    on_conflict: Optional[str] = None
) -> str:
    """
    Insert single row from TOON format.
    
    Args:
        table: Table name
        toon_string: TOON formatted string with row data
        schema: Schema name (default: 'public')
        on_conflict: PostgreSQL ON CONFLICT clause
    
    Returns:
        str: TOON formatted string with insert result (rowcount)
    """

def insert_many_from_toon(
    self, 
    table: str, 
    toon_string: str, 
    schema: str = 'public',
    on_conflict: Optional[str] = None
) -> str:
    """
    Insert multiple rows from TOON format using bulk INSERT.
    
    Args:
        table: Table name
        toon_string: TOON formatted string with list of rows
        schema: Schema name (default: 'public')
        on_conflict: PostgreSQL ON CONFLICT clause
    
    Returns:
        str: TOON formatted string with insert result (rowcount)
    """
```

### Update Operations

```python
def update_from_toon(
    self,
    table: str,
    toon_string: str,
    where: Dict[str, Any],
    schema: str = 'public'
) -> str:
    """
    Update rows from TOON format.
    
    Args:
        table: Table name
        toon_string: TOON formatted string with update data
        where: WHERE clause conditions as dict
        schema: Schema name (default: 'public')
    
    Returns:
        str: TOON formatted string with update result (rowcount)
    """
```

### Delete Operations

```python
def delete_from_toon(
    self,
    table: str,
    where: Dict[str, Any],
    schema: str = 'public'
) -> str:
    """
    Delete rows based on WHERE conditions.
    
    Args:
        table: Table name
        where: WHERE clause conditions as dict
        schema: Schema name (default: 'public')
    
    Returns:
        str: TOON formatted string with delete result (rowcount)
    """
```

## Testing Coverage

### Unit Tests

- ✅ `test_validate_table_name()` - Table name validation
- ✅ `test_generate_insert_sql_single_row()` - Single row INSERT SQL
- ✅ `test_generate_insert_sql_multiple_rows()` - Multi-row INSERT SQL
- ✅ `test_generate_update_sql()` - UPDATE SQL generation
- ✅ `test_generate_delete_sql()` - DELETE SQL generation
- ✅ `test_insert_one_from_toon()` - Insert single row
- ✅ `test_insert_many_from_toon()` - Insert multiple rows
- ✅ `test_update_from_toon()` - Update rows
- ✅ `test_delete_from_toon()` - Delete rows
- ✅ `test_convert_to_postgres_value_date_string()` - Date conversion
- ✅ `test_convert_to_postgres_value_datetime_string()` - Datetime conversion
- ✅ `test_convert_to_postgres_value_uuid_string()` - UUID conversion

### Integration Tests

- ✅ `test_insert_one_from_toon()` - Real database insert
- ✅ `test_insert_many_from_toon()` - Real database bulk insert
- ✅ `test_update_from_toon()` - Real database update
- ✅ `test_delete_from_toon()` - Real database delete

## Error Handling

All operations handle errors consistently:

1. **ConnectionError**: Connection issues, rollback transaction
2. **QueryError**: SQL execution errors, rollback transaction
3. **SchemaError**: Table/column validation failures
4. **SecurityError**: Invalid table/column names
5. **ValueError**: Invalid input data

## Security Best Practices

1. ✅ **Always Parameterize**: All SQL uses `%s` placeholders
2. ✅ **Validate Table Names**: Regex validation prevents injection
3. ✅ **Validate Columns**: Schema lookup verifies columns exist
4. ✅ **Type Conversion**: Safe type conversion with error handling
5. ✅ **Transaction Management**: Automatic rollback on errors

## Progress Tracking

| Phase | Operations | Status | Completion |
|-------|------------|--------|------------|
| Phase 1: Core Read | 4 operations | ✅ Complete | 4/4 (100%) |
| Phase 1: Core Write | 4 operations | ✅ Complete | 4/4 (100%) |
| **Total** | **8 operations** | **✅ Complete** | **8/8 (100%)** |

### Phase 1 Status: ✅ COMPLETE

**Implemented Operations:**
- ✅ `query()` - SQL → TOON
- ✅ `execute()` - SQL → TOON
- ✅ `get_schema()` - SQL → TOON
- ✅ `get_tables()` - SQL → TOON
- ✅ `insert_one_from_toon()` - TOON → SQL → PostgreSQL
- ✅ `insert_many_from_toon()` - TOON → SQL → PostgreSQL
- ✅ `update_from_toon()` - TOON → SQL → PostgreSQL
- ✅ `delete_from_toon()` - JSON → SQL → PostgreSQL

## Notes

- All write operations return results in TOON format for consistency
- Error handling is consistent across all operations
- All operations support schema parameter (default: 'public')
- Type cleaning handles all PostgreSQL BSON types
- SQL generation is always parameterized for security
- Schema validation ensures columns exist before operations

## References

- [PostgreSQL Python Driver Documentation](https://www.psycopg.org/docs/)
- [PostgreSQL SQL Reference](https://www.postgresql.org/docs/current/sql.html)
- [TOON Format Specification](https://github.com/xaviviro/python-toon)

