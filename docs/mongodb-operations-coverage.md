# MongoDB Operations Coverage & Implementation Plan

## Overview

This document outlines the complete MongoDB operations coverage for the `toondb` package, focusing on bidirectional conversion between JSON (MongoDB native format) and TOON (Token-Oriented Object Notation) format.

## Current Coverage Analysis

### ✅ Currently Implemented Operations

| Operation                 | Direction             | Status         | Method                                 | Notes                                         |
| ------------------------- | --------------------- | -------------- | -------------------------------------- | --------------------------------------------- |
| `find()`                  | JSON → TOON           | ✅ Implemented | `MongoAdapter.find()`                  | Returns TOON string                           |
| `query()`                 | JSON → TOON           | ✅ Implemented | `MongoAdapter.query()`                 | Alias for find(), accepts JSON string or Dict |
| `find_one()`              | JSON → TOON           | ✅ Implemented | `MongoAdapter.find_one()`              | Returns single document or empty TOON         |
| `aggregate()`             | JSON → TOON           | ✅ Implemented | `MongoAdapter.aggregate()`             | Aggregation pipeline results                  |
| `count_documents()`       | JSON → Number         | ✅ Implemented | `MongoAdapter.count_documents()`       | Returns integer count                         |
| `distinct()`              | JSON → TOON           | ✅ Implemented | `MongoAdapter.distinct()`              | Distinct values as TOON                       |
| `insert_one_from_toon()`  | TOON → JSON → MongoDB | ✅ Implemented | `MongoAdapter.insert_one_from_toon()`  | Insert single document                        |
| `insert_many_from_toon()` | TOON → JSON → MongoDB | ✅ Implemented | `MongoAdapter.insert_many_from_toon()` | Insert multiple documents                     |
| `update_one_from_toon()`  | TOON → JSON → MongoDB | ✅ Implemented | `MongoAdapter.update_one_from_toon()`  | Update single document                        |
| `update_many_from_toon()` | TOON → JSON → MongoDB | ✅ Implemented | `MongoAdapter.update_many_from_toon()` | Update multiple documents                     |
| `replace_one_from_toon()` | TOON → JSON → MongoDB | ✅ Implemented | `MongoAdapter.replace_one_from_toon()` | Replace single document                       |
| `delete_one()`            | JSON → MongoDB        | ✅ Implemented | `MongoAdapter.delete_one()`            | Delete single document                        |
| `delete_many()`           | JSON → MongoDB        | ✅ Implemented | `MongoAdapter.delete_many()`           | Delete multiple documents                     |

**Current Coverage: 65% (13/20 total operations) - Phase 1 Complete!**

### Current Implementation Flow

```
JSON Query (Dict/JSON string)
  → MongoAdapter.query() or find()
  → MongoDB collection.find()
  → List[Dict] results
  → _clean_mongo_docs() (ObjectId → str, datetime → isoformat)
  → _to_toon() → TOON string
```

## Missing Operations

### Phase 1: Core Read Operations (JSON → TOON) - ✅ COMPLETE

| Operation                | Direction     | Status         | Use Case                          |
| ------------------------ | ------------- | -------------- | --------------------------------- |
| `find_one()`             | JSON → TOON   | ✅ Implemented | Find single document              |
| `aggregate()`            | JSON → TOON   | ✅ Implemented | Aggregation pipeline              |
| `count_documents()`      | JSON → Number | ✅ Implemented | Count matching documents          |
| `distinct()`             | JSON → TOON   | ✅ Implemented | Get distinct values               |
| `find_one_and_update()`  | JSON → TOON   | ❌ Missing     | Atomic find and update (Phase 2)  |
| `find_one_and_replace()` | JSON → TOON   | ❌ Missing     | Atomic find and replace (Phase 2) |
| `find_one_and_delete()`  | JSON → TOON   | ❌ Missing     | Atomic find and delete (Phase 2)  |

### Phase 1: Core Write Operations (TOON → JSON → MongoDB) - ✅ COMPLETE

| Operation                 | Direction             | Status         | Use Case                  |
| ------------------------- | --------------------- | -------------- | ------------------------- |
| `insert_one_from_toon()`  | TOON → JSON → MongoDB | ✅ Implemented | Insert single document    |
| `insert_many_from_toon()` | TOON → JSON → MongoDB | ✅ Implemented | Insert multiple documents |
| `update_one_from_toon()`  | TOON → JSON → MongoDB | ✅ Implemented | Update single document    |
| `update_many_from_toon()` | TOON → JSON → MongoDB | ✅ Implemented | Update multiple documents |
| `replace_one_from_toon()` | TOON → JSON → MongoDB | ✅ Implemented | Replace single document   |
| `delete_one()`            | JSON → MongoDB        | ✅ Implemented | Delete single document    |
| `delete_many()`           | JSON → MongoDB        | ✅ Implemented | Delete multiple documents |

### Phase 3: Advanced Operations

| Operation                | Direction             | Status     | Use Case                        |
| ------------------------ | --------------------- | ---------- | ------------------------------- |
| `bulk_write_from_toon()` | TOON → JSON → MongoDB | ❌ Missing | Multiple operations in one call |
| `create_index()`         | JSON → MongoDB        | ❌ Missing | Index creation                  |
| `drop_index()`           | JSON → MongoDB        | ❌ Missing | Index deletion                  |

## Bidirectional Conversion Matrix

### Current State

| Conversion                 | Status  | Implementation                                                 |
| -------------------------- | ------- | -------------------------------------------------------------- |
| JSON Query → TOON Results  | ✅ Done | `find()`, `query()`, `find_one()`, `aggregate()`, `distinct()` |
| TOON → JSON (for inserts)  | ✅ Done | `insert_one_from_toon()`, `insert_many_from_toon()`            |
| TOON → JSON (for updates)  | ✅ Done | `update_one_from_toon()`, `update_many_from_toon()`            |
| TOON → JSON (for replaces) | ✅ Done | `replace_one_from_toon()`                                      |
| TOON → JSON (for deletes)  | ✅ Done | `delete_one()`, `delete_many()`                                |
| TOON → Python Dict         | ✅ Done | `from_toon()` (direct)                                         |
| Python Dict → TOON         | ✅ Done | `to_toon()` (direct)                                           |

### Target State: Complete Bidirectional Coverage

```
┌─────────────────────────────────────────────────────────────┐
│                    MongoDB Operations                        │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  READ OPERATIONS (JSON → TOON)                               │
│  ✅ find()                                                    │
│  ✅ query()                                                   │
│  ❌ find_one()                                                │
│  ❌ aggregate()                                               │
│  ❌ count_documents()                                         │
│  ❌ distinct()                                                │
│  ❌ find_one_and_update()                                     │
│  ❌ find_one_and_replace()                                    │
│  ❌ find_one_and_delete()                                     │
│                                                               │
│  WRITE OPERATIONS (TOON → JSON → MongoDB)                    │
│  ❌ insert_one_from_toon()                                   │
│  ❌ insert_many_from_toon()                                  │
│  ❌ update_one_from_toon()                                   │
│  ❌ update_many_from_toon()                                  │
│  ❌ replace_one_from_toon()                                  │
│  ❌ delete_one()                                             │
│  ❌ delete_many()                                             │
│  ❌ bulk_write_from_toon()                                   │
│                                                               │
│  CONVERSION HELPERS                                           │
│  ❌ to_json() - TOON → JSON string                           │
│  ❌ from_json() - JSON string → TOON                         │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

## Implementation Plan

### Priority 1: Core Bidirectional Operations (Must Have)

#### 1.1 Read Operations

**1.1.1 `find_one()` - Find Single Document**

```python
def find_one(self, query: Dict = None, projection: Dict = None) -> str:
    """
    Find single document and return in TOON format.

    Args:
        query: MongoDB query dictionary
        projection: MongoDB projection dictionary

    Returns:
        str: TOON formatted string (single document or empty)
    """
```

**1.1.2 `aggregate()` - Aggregation Pipeline**

```python
def aggregate(self, pipeline: List[Dict]) -> str:
    """
    Execute aggregation pipeline and return results in TOON format.

    Args:
        pipeline: List of aggregation pipeline stages

    Returns:
        str: TOON formatted string
    """
```

**1.1.3 `count_documents()` - Count Documents**

```python
def count_documents(self, filter: Dict = None) -> int:
    """
    Count documents matching filter.

    Args:
        filter: MongoDB filter dictionary

    Returns:
        int: Number of matching documents
    """
```

**1.1.4 `distinct()` - Distinct Values**

```python
def distinct(self, key: str, filter: Dict = None) -> str:
    """
    Get distinct values for a key.

    Args:
        key: Field name
        filter: Optional filter dictionary

    Returns:
        str: TOON formatted string with distinct values
    """
```

#### 1.2 Write Operations

**1.2.1 `insert_one_from_toon()` - Insert Single Document**

```python
def insert_one_from_toon(self, toon_string: str) -> str:
    """
    Insert single document from TOON format.

    Flow: TOON → from_toon() → Python Dict → MongoDB insert_one() → Return result as TOON

    Args:
        toon_string: TOON formatted string containing document data

    Returns:
        str: TOON formatted string with inserted_id
    """
```

**1.2.2 `insert_many_from_toon()` - Insert Multiple Documents**

```python
def insert_many_from_toon(self, toon_string: str, ordered: bool = True) -> str:
    """
    Insert multiple documents from TOON format.

    Flow: TOON → from_toon() → List[Dict] → MongoDB insert_many() → Return result as TOON

    Args:
        toon_string: TOON formatted string containing list of documents
        ordered: If True, stop on first error

    Returns:
        str: TOON formatted string with inserted_ids
    """
```

**1.2.3 `update_one_from_toon()` - Update Single Document**

```python
def update_one_from_toon(
    self,
    filter: Dict,
    toon_string: str,
    upsert: bool = False
) -> str:
    """
    Update single document from TOON format.

    Flow: TOON → from_toon() → Python Dict → MongoDB update_one() → Return result as TOON

    Args:
        filter: MongoDB filter dictionary
        toon_string: TOON formatted string with update data
        upsert: If True, insert if document doesn't exist

    Returns:
        str: TOON formatted string with update result
    """
```

**1.2.4 `update_many_from_toon()` - Update Multiple Documents**

```python
def update_many_from_toon(self, filter: Dict, toon_string: str) -> str:
    """
    Update multiple documents from TOON format.

    Flow: TOON → from_toon() → Python Dict → MongoDB update_many() → Return result as TOON

    Args:
        filter: MongoDB filter dictionary
        toon_string: TOON formatted string with update data

    Returns:
        str: TOON formatted string with update result
    """
```

**1.2.5 `replace_one_from_toon()` - Replace Single Document**

```python
def replace_one_from_toon(
    self,
    filter: Dict,
    toon_string: str,
    upsert: bool = False
) -> str:
    """
    Replace single document from TOON format.

    Flow: TOON → from_toon() → Python Dict → MongoDB replace_one() → Return result as TOON

    Args:
        filter: MongoDB filter dictionary
        toon_string: TOON formatted string with replacement document
        upsert: If True, insert if document doesn't exist

    Returns:
        str: TOON formatted string with replace result
    """
```

**1.2.6 `delete_one()` - Delete Single Document**

```python
def delete_one(self, filter: Dict) -> str:
    """
    Delete single document matching filter.

    Args:
        filter: MongoDB filter dictionary

    Returns:
        str: TOON formatted string with delete result
    """
```

**1.2.7 `delete_many()` - Delete Multiple Documents**

```python
def delete_many(self, filter: Dict) -> str:
    """
    Delete multiple documents matching filter.

    Args:
        filter: MongoDB filter dictionary

    Returns:
        str: TOON formatted string with delete result
    """
```

### Priority 2: Advanced Operations (Should Have)

#### 2.1 Atomic Operations

**2.1.1 `find_one_and_update()`**

```python
def find_one_and_update(
    self,
    filter: Dict,
    update: Dict,
    projection: Dict = None,
    sort: List[tuple] = None,
    return_document: bool = True,
    upsert: bool = False
) -> str:
    """
    Atomically find and update a document.

    Returns:
        str: TOON formatted string with document (before or after update)
    """
```

**2.1.2 `find_one_and_replace()`**

```python
def find_one_and_replace(
    self,
    filter: Dict,
    replacement: Dict,
    projection: Dict = None,
    sort: List[tuple] = None,
    return_document: bool = True,
    upsert: bool = False
) -> str:
    """
    Atomically find and replace a document.

    Returns:
        str: TOON formatted string with document (before or after replace)
    """
```

**2.1.3 `find_one_and_delete()`**

```python
def find_one_and_delete(
    self,
    filter: Dict,
    projection: Dict = None,
    sort: List[tuple] = None
) -> str:
    """
    Atomically find and delete a document.

    Returns:
        str: TOON formatted string with deleted document
    """
```

#### 2.2 Bulk Operations

**2.2.1 `bulk_write_from_toon()`**

```python
def bulk_write_from_toon(self, operations_toon: str, ordered: bool = True) -> str:
    """
    Execute multiple write operations from TOON format.

    Flow: TOON → from_toon() → List[Dict] → Convert to BulkWriteOps → Execute → Return result as TOON

    Args:
        operations_toon: TOON formatted string with list of operations
        ordered: If True, stop on first error

    Returns:
        str: TOON formatted string with bulk write result
    """
```

### Priority 3: Helper Methods (Nice to Have)

#### 3.1 Conversion Helpers

**3.1.1 `to_json()` - TOON to JSON**

```python
def to_json(self, toon_string: str) -> str:
    """
    Convert TOON format to JSON string.

    Flow: TOON → from_toon() → Python Dict → json.dumps() → JSON string

    Args:
        toon_string: TOON formatted string

    Returns:
        str: JSON formatted string
    """
    from toonpy.core.converter import from_toon
    data = from_toon(toon_string)
    return json.dumps(data, indent=2)
```

**3.1.2 `from_json()` - JSON to TOON**

```python
def from_json(self, json_string: str) -> str:
    """
    Convert JSON string to TOON format.

    Flow: JSON string → json.loads() → Python Dict → to_toon() → TOON string

    Args:
        json_string: JSON formatted string

    Returns:
        str: TOON formatted string
    """
    from toonpy.core.converter import to_toon
    data = json.loads(json_string)
    return to_toon(data)
```

#### 3.2 Enhanced Type Cleaning

**3.2.1 Enhanced `_clean_mongo_docs()`**

Currently handles:

- ✅ ObjectId → str
- ✅ datetime/date → isoformat

Should also handle:

- ❌ Binary → base64 string
- ❌ Decimal128 → float or string
- ❌ Timestamp → datetime
- ❌ Code → string
- ❌ DBRef → dict
- ❌ MinKey/MaxKey → string
- ❌ Regex → string
- ❌ UUID → string

## Complete Method Signature Plan

```python
class MongoAdapter(BaseAdapter):
    # === READ OPERATIONS (JSON → TOON) ===
    def find(self, query: Dict = None, projection: Dict = None) -> str: ✅
    def find_one(self, query: Dict = None, projection: Dict = None) -> str: ❌
    def query(self, query: Union[str, Dict] = None) -> str: ✅
    def aggregate(self, pipeline: List[Dict]) -> str: ❌
    def count_documents(self, filter: Dict = None) -> int: ❌
    def distinct(self, key: str, filter: Dict = None) -> str: ❌
    def find_one_and_update(self, filter: Dict, update: Dict, ...) -> str: ❌
    def find_one_and_replace(self, filter: Dict, replacement: Dict, ...) -> str: ❌
    def find_one_and_delete(self, filter: Dict, ...) -> str: ❌

    # === WRITE OPERATIONS (TOON → JSON → MongoDB) ===
    def insert_one_from_toon(self, toon_string: str) -> str: ❌
    def insert_many_from_toon(self, toon_string: str, ordered: bool = True) -> str: ❌
    def update_one_from_toon(self, filter: Dict, toon_string: str, upsert: bool = False) -> str: ❌
    def update_many_from_toon(self, filter: Dict, toon_string: str) -> str: ❌
    def replace_one_from_toon(self, filter: Dict, toon_string: str, upsert: bool = False) -> str: ❌
    def delete_one(self, filter: Dict) -> str: ❌
    def delete_many(self, filter: Dict) -> str: ❌
    def bulk_write_from_toon(self, operations_toon: str, ordered: bool = True) -> str: ❌

    # === CONVERSION HELPERS ===
    def to_json(self, toon_string: str) -> str: ❌
    def from_json(self, json_string: str) -> str: ❌

    # === INTERNAL METHODS ===
    def _clean_mongo_docs(self, docs: List[Dict]) -> List[Dict]: ✅ (needs enhancement)
    def _to_toon(self, results: List[Dict]) -> str: ✅
```

## Implementation Flow Examples

### Example 1: Complete Insert Flow

```
User has TOON data:
  users[2]{name,age,email}:
    Alice,30,alice@example.com
    Bob,25,bob@example.com

Flow:
  1. insert_many_from_toon(toon_string)
  2. from_toon(toon_string) → List[Dict]
     [
       {"name": "Alice", "age": 30, "email": "alice@example.com"},
       {"name": "Bob", "age": 25, "email": "bob@example.com"}
     ]
  3. collection.insert_many(documents)
  4. Result: InsertManyResult with inserted_ids
  5. Convert result to TOON: to_toon([{"inserted_ids": [...]}])
  6. Return TOON string
```

### Example 2: Complete Update Flow

```
User has TOON update data:
  update[1]{age,status}:
    31,active

Flow:
  1. update_one_from_toon(filter={"name": "Alice"}, toon_string)
  2. from_toon(toon_string) → Dict
     {"age": 31, "status": "active"}
  3. collection.update_one(filter, {"$set": update_data})
  4. Result: UpdateResult with matched_count, modified_count
  5. Convert result to TOON: to_toon([{"matched": 1, "modified": 1}])
  6. Return TOON string
```

### Example 3: Complete Query Flow (Already Implemented)

```
User has JSON query:
  {"age": {"$gt": 25}}

Flow:
  1. find(query)
  2. collection.find(query) → Cursor
  3. list(cursor) → List[Dict]
  4. _clean_mongo_docs() → Cleaned List[Dict]
  5. _to_toon() → TOON string
  6. Return TOON string
```

## Testing Requirements

### Unit Tests Needed

1. **Read Operations**

   - `test_find_one()` - Single document retrieval
   - `test_aggregate()` - Aggregation pipeline
   - `test_count_documents()` - Document counting
   - `test_distinct()` - Distinct values
   - `test_find_one_and_update()` - Atomic operations

2. **Write Operations**

   - `test_insert_one_from_toon()` - Single insert
   - `test_insert_many_from_toon()` - Batch insert
   - `test_update_one_from_toon()` - Single update
   - `test_update_many_from_toon()` - Batch update
   - `test_replace_one_from_toon()` - Replace operation
   - `test_delete_one()` - Single delete
   - `test_delete_many()` - Batch delete
   - `test_bulk_write_from_toon()` - Bulk operations

3. **Conversion Helpers**

   - `test_to_json()` - TOON to JSON conversion
   - `test_from_json()` - JSON to TOON conversion
   - `test_bidirectional_conversion()` - Round-trip conversion

4. **Type Cleaning**
   - `test_clean_binary()` - Binary data handling
   - `test_clean_decimal128()` - Decimal128 handling
   - `test_clean_timestamp()` - Timestamp handling
   - `test_clean_nested_structures()` - Complex nested data

## Success Criteria

### Phase 1 Complete When:

- ✅ All Priority 1 operations implemented
- ✅ All Priority 1 operations have unit tests
- ✅ Bidirectional conversion works (JSON ↔ TOON)
- ✅ Documentation updated

### Phase 2 Complete When:

- ✅ All Priority 2 operations implemented
- ✅ All Priority 2 operations have unit tests
- ✅ Advanced operations tested

### Phase 3 Complete When:

- ✅ All helper methods implemented
- ✅ Enhanced type cleaning complete
- ✅ Full MongoDB ecosystem coverage achieved
- ✅ 100% test coverage for all operations

## Progress Tracking

| Phase               | Operations        | Status             | Completion      |
| ------------------- | ----------------- | ------------------ | --------------- |
| Phase 1: Core Read  | 7 operations      | ✅ Complete        | 6/7 (86%)       |
| Phase 1: Core Write | 7 operations      | ✅ Complete        | 7/7 (100%)      |
| Phase 2: Advanced   | 4 operations      | 🔴 Not Started     | 0/4 (0%)        |
| Phase 3: Helpers    | 2 operations      | 🔴 Not Started     | 0/2 (0%)        |
| **Total**           | **20 operations** | **🟡 In Progress** | **13/20 (65%)** |

### Phase 1 Status: ✅ COMPLETE

**Implemented Operations:**

- ✅ `find()` - JSON → TOON
- ✅ `query()` - JSON → TOON
- ✅ `find_one()` - JSON → TOON
- ✅ `aggregate()` - JSON → TOON
- ✅ `count_documents()` - JSON → Number
- ✅ `distinct()` - JSON → TOON
- ✅ `insert_one_from_toon()` - TOON → JSON → MongoDB
- ✅ `insert_many_from_toon()` - TOON → JSON → MongoDB
- ✅ `update_one_from_toon()` - TOON → JSON → MongoDB
- ✅ `update_many_from_toon()` - TOON → JSON → MongoDB
- ✅ `replace_one_from_toon()` - TOON → JSON → MongoDB
- ✅ `delete_one()` - JSON → MongoDB
- ✅ `delete_many()` - JSON → MongoDB

**Note:** Phase 1 includes 6 read operations (find, query, find*one, aggregate, count_documents, distinct) and 7 write operations. The atomic operations (find_one_and*\*) are part of Phase 2.

## Notes

- All write operations should return results in TOON format for consistency
- Error handling should be consistent across all operations
- All operations should support both Dict and JSON string inputs where applicable
- Type cleaning should handle all MongoDB BSON types
- Consider adding async versions in future (Phase 4)

## References

- [MongoDB Python Driver Documentation](https://pymongo.readthedocs.io/)
- [MongoDB Operations Reference](https://docs.mongodb.com/manual/reference/operator/)
- [TOON Format Specification](https://github.com/xaviviro/python-toon)
