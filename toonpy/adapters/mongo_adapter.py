from toonpy.adapters.base import BaseAdapter
from typing import Union, Optional, Dict, Any, List
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime, date, time
import json


class MongoAdapter(BaseAdapter):
    """Adapter for MongoDB"""
    
    def __init__(
        self,
        connection_string: Optional[str] = None,
        collection: Optional[str] = None,
        database: Optional[str] = None, 
        collection_name: Optional[str] = None
    ):

        """
        Initialize MongoDB Adapter

        Args: 
            connection_string: MongoDB connection string
            collection: MongoDB collection name
            database: MongoDB database name
            collection_name: Name of the collection to use for TOON encoding
        """

        if collection is not None: 
            self.collection = collection
            self.own_connection = False
        elif connection_string and database and collection_name:
            client = MongoClient(connection_string)
            self.collection = client[database][collection_name]
            self.own_connection = True
        else:
            raise ValueError("Invalid configuration. Provide either collection or connection_string, database, and collection_name.")
    
    def find(self, query: Dict = None, projection: Dict = None) -> str:
        """
        Execute MongoDB find query and return results in TOON format

        Args:
            query: MongoDB query dictionary
            projection: MongoDB projection dictionary

        Returns:
            str: TOON formatted string
        """
        if query is None:
            query  = {}
        
        cursor = self.collection.find(query, projection)
        results = list(cursor)

        data = self._clean_mongo_docs(results)

        return self._to_toon(data)
    
    def query(self, query: Union[str, Dict] = None) -> str:
        """
        Execute query (implements abstract method from BaseAdapter).
        Accepts either a JSON string or a dictionary for MongoDB query.
        
        Args:
            query: MongoDB query as JSON string or dictionary
        
        Returns:
            str: TOON formatted string
        """
        if query is None:
            query = {}
        elif isinstance(query, str):
            # Parse JSON string to dict
            query = json.loads(query)
        
        return self.find(query)

    def find_one(self, query: Dict = None, projection: Dict = None) -> str:
        """
        Find single document and return in TOON format.
        
        Args:
            query: MongoDB query dictionary
            projection: MongoDB projection dictionary
        
        Returns:
            str: TOON formatted string (single document or empty)
        """
        if query is None:
            query = {}
        
        result = self.collection.find_one(query, projection)
        
        if result is None:
            return self._to_toon([])
        
        data = self._clean_mongo_docs([result])
        return self._to_toon(data)
    
    def aggregate(self, pipeline: List[Dict]) -> str:
        """
        Execute aggregation pipeline and return results in TOON format.
        
        Args:
            pipeline: List of aggregation pipeline stages
        
        Returns:
            str: TOON formatted string
        """
        cursor = self.collection.aggregate(pipeline)
        results = list(cursor)
        
        data = self._clean_mongo_docs(results)
        return self._to_toon(data)
    
    def count_documents(self, filter: Dict = None) -> int:
        """
        Count documents matching filter.
        
        Args:
            filter: MongoDB filter dictionary
        
        Returns:
            int: Number of matching documents
        """
        if filter is None:
            filter = {}
        
        return self.collection.count_documents(filter)
    
    def distinct(self, key: str, filter: Dict = None) -> str:
        """
        Get distinct values for a key.
        
        Args:
            key: Field name
            filter: Optional filter dictionary
        
        Returns:
            str: TOON formatted string with distinct values
        """
        if filter is None:
            filter = {}
        
        distinct_values = self.collection.distinct(key, filter)
        
        # Convert to list of dicts for consistent TOON format
        data = [{key: value} for value in distinct_values]
        return self._to_toon(data)
    
    def insert_one_from_toon(self, toon_string: str) -> str:
        """
        Insert single document from TOON format.
        
        Flow: TOON → from_toon() → Python Dict → MongoDB insert_one() → Return result as TOON
        
        Args:
            toon_string: TOON formatted string containing document data
        
        Returns:
            str: TOON formatted string with inserted_id
        """
        from toonpy.core.converter import from_toon
        
        data = from_toon(toon_string)
        
        # Handle both single dict and list with one dict
        if isinstance(data, list):
            if len(data) == 0:
                raise ValueError("TOON string must contain at least one document")
            document = data[0]
        elif isinstance(data, dict):
            document = data
        else:
            raise ValueError(f"TOON string must decode to a dict or list of dicts, got {type(data)}")
        
        result = self.collection.insert_one(document)
        
        # Return result as TOON
        result_dict = {
            "inserted_id": str(result.inserted_id),
            "acknowledged": result.acknowledged
        }
        return self._to_toon([result_dict])
    
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
        from toonpy.core.converter import from_toon
        
        data = from_toon(toon_string)
        
        # Ensure data is a list
        if not isinstance(data, list):
            data = [data]
        
        if len(data) == 0:
            raise ValueError("TOON string must contain at least one document")
        
        result = self.collection.insert_many(data, ordered=ordered)
        
        # Return result as TOON
        result_dict = {
            "inserted_ids": [str(id) for id in result.inserted_ids],
            "acknowledged": result.acknowledged
        }
        return self._to_toon([result_dict])
    
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
        from toonpy.core.converter import from_toon
        
        update_data = from_toon(toon_string)
        
        # Handle both single dict and list with one dict
        if isinstance(update_data, list):
            if len(update_data) == 0:
                raise ValueError("TOON string must contain update data")
            update_dict = update_data[0]
        elif isinstance(update_data, dict):
            update_dict = update_data
        else:
            raise ValueError(f"TOON string must decode to a dict or list of dicts, got {type(update_data)}")
        
        # Wrap in $set if not already an update operator
        if not any(key.startswith("$") for key in update_dict.keys()):
            update_dict = {"$set": update_dict}
        
        result = self.collection.update_one(filter, update_dict, upsert=upsert)
        
        # Return result as TOON
        result_dict = {
            "matched_count": result.matched_count,
            "modified_count": result.modified_count,
            "upserted_id": str(result.upserted_id) if result.upserted_id else None,
            "acknowledged": result.acknowledged
        }
        return self._to_toon([result_dict])
    
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
        from toonpy.core.converter import from_toon
        
        update_data = from_toon(toon_string)
        
        # Handle both single dict and list with one dict
        if isinstance(update_data, list):
            if len(update_data) == 0:
                raise ValueError("TOON string must contain update data")
            update_dict = update_data[0]
        elif isinstance(update_data, dict):
            update_dict = update_data
        else:
            raise ValueError(f"TOON string must decode to a dict or list of dicts, got {type(update_data)}")
        
        # Wrap in $set if not already an update operator
        if not any(key.startswith("$") for key in update_dict.keys()):
            update_dict = {"$set": update_dict}
        
        result = self.collection.update_many(filter, update_dict)
        
        # Return result as TOON
        result_dict = {
            "matched_count": result.matched_count,
            "modified_count": result.modified_count,
            "acknowledged": result.acknowledged
        }
        return self._to_toon([result_dict])
    
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
        from toonpy.core.converter import from_toon
        
        replacement = from_toon(toon_string)
        
        # Handle both single dict and list with one dict
        if isinstance(replacement, list):
            if len(replacement) == 0:
                raise ValueError("TOON string must contain replacement document")
            replacement_doc = replacement[0]
        elif isinstance(replacement, dict):
            replacement_doc = replacement
        else:
            raise ValueError(f"TOON string must decode to a dict or list of dicts, got {type(replacement)}")
        
        result = self.collection.replace_one(filter, replacement_doc, upsert=upsert)
        
        # Return result as TOON
        result_dict = {
            "matched_count": result.matched_count,
            "modified_count": result.modified_count,
            "upserted_id": str(result.upserted_id) if result.upserted_id else None,
            "acknowledged": result.acknowledged
        }
        return self._to_toon([result_dict])
    
    def insert_and_query_from_toon(
        self, 
        toon_string: str, 
        query_filter: Optional[Dict] = None,
        projection: Optional[Dict] = None
    ) -> str:
        """
        Insert TOON data and immediately query it back as TOON.
        Uses the same instance/session - guaranteed to work.
        
        Flow: TOON → insert → query back → TOON (all in same instance)
        
        Args:
            toon_string: TOON formatted string containing document data
            query_filter: Optional MongoDB filter to query back inserted document.
                         If None, queries by inserted_id
            projection: Optional MongoDB projection dictionary
        
        Returns:
            str: TOON formatted string with queried document data
        
        Example:
            >>> adapter = MongoAdapter(...)
            >>> toon_data = to_toon([{"name": "Alice", "age": 30}])
            >>> result = adapter.insert_and_query_from_toon(toon_data)
            >>> # Returns TOON with the inserted document (with _id)
        """
        from toonpy.core.converter import from_toon
        
        # Parse TOON input
        data = from_toon(toon_string)
        
        # Handle both single dict and list with one dict
        if isinstance(data, list):
            if len(data) == 0:
                raise ValueError("TOON string must contain at least one document")
            document = data[0]
        elif isinstance(data, dict):
            document = data
        else:
            raise ValueError(f"TOON string must decode to a dict or list of dicts, got {type(data)}")
        
        # Insert using same instance (self.collection)
        result = self.collection.insert_one(document)
        inserted_id = result.inserted_id
        
        # Query back using same instance
        if query_filter is None:
            # Default: query by inserted_id
            query_filter = {"_id": inserted_id}
        
        # Use same instance to query back
        queried_doc = self.collection.find_one(query_filter, projection)
        
        if queried_doc is None:
            return self._to_toon([])
        
        # Clean and return as TOON using same instance
        cleaned = self._clean_mongo_docs([queried_doc])
        return self._to_toon(cleaned)
    
    def insert_many_and_query_from_toon(
        self, 
        toon_string: str, 
        query_filter: Optional[Dict] = None,
        projection: Optional[Dict] = None,
        limit: Optional[int] = None
    ) -> str:
        """
        Insert multiple TOON documents and immediately query them back as TOON.
        Uses the same instance/session - guaranteed to work.
        
        Flow: TOON → bulk insert → query back → TOON (all in same instance)
        
        Args:
            toon_string: TOON formatted string containing list of documents
            query_filter: Optional MongoDB filter to query back inserted documents.
                         If None, queries by inserted_ids
            projection: Optional MongoDB projection dictionary
            limit: Optional limit on number of documents to return
        
        Returns:
            str: TOON formatted string with queried documents
        
        Example:
            >>> adapter = MongoAdapter(...)
            >>> toon_data = to_toon([{"name": "Alice"}, {"name": "Bob"}])
            >>> result = adapter.insert_many_and_query_from_toon(toon_data)
            >>> # Returns TOON with both inserted documents (with _ids)
        """
        from toonpy.core.converter import from_toon
        
        # Parse TOON input
        data = from_toon(toon_string)
        
        # Ensure data is a list
        if not isinstance(data, list):
            data = [data]
        
        if len(data) == 0:
            raise ValueError("TOON string must contain at least one document")
        
        # Insert using same instance (self.collection)
        result = self.collection.insert_many(data, ordered=True)
        inserted_ids = result.inserted_ids
        
        # Query back using same instance
        if query_filter is None:
            # Default: query by inserted_ids
            query_filter = {"_id": {"$in": inserted_ids}}
        
        # Use same instance to query back
        cursor = self.collection.find(query_filter, projection)
        
        if limit is not None:
            cursor = cursor.limit(limit)
        
        queried_docs = list(cursor)
        
        if not queried_docs:
            return self._to_toon([])
        
        # Clean and return as TOON using same instance
        cleaned = self._clean_mongo_docs(queried_docs)
        return self._to_toon(cleaned)
    
    def update_and_query_from_toon(
        self,
        filter: Dict,
        toon_string: str,
        projection: Optional[Dict] = None,
        upsert: bool = False
    ) -> str:
        """
        Update document from TOON and immediately query it back as TOON.
        Uses the same instance/session - guaranteed to work.
        
        Flow: TOON → update → query back → TOON (all in same instance)
        
        Args:
            filter: MongoDB filter dictionary to find document to update
            toon_string: TOON formatted string with update data
            projection: Optional MongoDB projection dictionary
            upsert: If True, insert if document doesn't exist
        
        Returns:
            str: TOON formatted string with updated document data
        
        Example:
            >>> adapter = MongoAdapter(...)
            >>> update_data = to_toon([{"age": 31, "status": "active"}])
            >>> result = adapter.update_and_query_from_toon(
            ...     {"name": "Alice"}, 
            ...     update_data
            ... )
            >>> # Returns TOON with updated document
        """
        from toonpy.core.converter import from_toon
        
        # Update using existing method (uses same instance)
        update_result = self.update_one_from_toon(filter, toon_string, upsert=upsert)
        
        # Extract matched/upserted ID from update result
        result_data = from_toon(update_result)
        if result_data and len(result_data) > 0:
            result_dict = result_data[0]
            upserted_id = result_dict.get("upserted_id")
            
            # Determine what to query
            if upserted_id:
                # Document was inserted (upsert), query by upserted_id
                from bson import ObjectId
                query_filter = {"_id": ObjectId(upserted_id)}
            else:
                # Document was updated, use original filter
                query_filter = filter
            
            # Query back using same instance
            queried_doc = self.collection.find_one(query_filter, projection)
            
            if queried_doc is None:
                return self._to_toon([])
            
            # Clean and return as TOON using same instance
            cleaned = self._clean_mongo_docs([queried_doc])
            return self._to_toon(cleaned)
        
        return self._to_toon([])
    
    def replace_and_query_from_toon(
        self,
        filter: Dict,
        toon_string: str,
        projection: Optional[Dict] = None,
        upsert: bool = False
    ) -> str:
        """
        Replace document from TOON and immediately query it back as TOON.
        Uses the same instance/session - guaranteed to work.
        
        Flow: TOON → replace → query back → TOON (all in same instance)
        
        Args:
            filter: MongoDB filter dictionary to find document to replace
            toon_string: TOON formatted string with replacement document
            projection: Optional MongoDB projection dictionary
            upsert: If True, insert if document doesn't exist
        
        Returns:
            str: TOON formatted string with replaced document data
        
        Example:
            >>> adapter = MongoAdapter(...)
            >>> replacement = to_toon([{"name": "Alice Updated", "age": 32}])
            >>> result = adapter.replace_and_query_from_toon(
            ...     {"name": "Alice"}, 
            ...     replacement
            ... )
            >>> # Returns TOON with replaced document
        """
        from toonpy.core.converter import from_toon
        
        # Replace using existing method (uses same instance)
        replace_result = self.replace_one_from_toon(filter, toon_string, upsert=upsert)
        
        # Extract matched/upserted ID from replace result
        result_data = from_toon(replace_result)
        if result_data and len(result_data) > 0:
            result_dict = result_data[0]
            upserted_id = result_dict.get("upserted_id")
            
            # Determine what to query
            if upserted_id:
                # Document was inserted (upsert), query by upserted_id
                from bson import ObjectId
                query_filter = {"_id": ObjectId(upserted_id)}
            else:
                # Document was replaced, use original filter
                query_filter = filter
            
            # Query back using same instance
            queried_doc = self.collection.find_one(query_filter, projection)
            
            if queried_doc is None:
                return self._to_toon([])
            
            # Clean and return as TOON using same instance
            cleaned = self._clean_mongo_docs([queried_doc])
            return self._to_toon(cleaned)
    
    def delete_one(self, filter: Dict) -> str:
        """
        Delete single document matching filter.
        
        Args:
            filter: MongoDB filter dictionary
        
        Returns:
            str: TOON formatted string with delete result
        """
        result = self.collection.delete_one(filter)
        
        # Return result as TOON
        result_dict = {
            "deleted_count": result.deleted_count,
            "acknowledged": result.acknowledged
        }
        return self._to_toon([result_dict])
    
    def delete_many(self, filter: Dict) -> str:
        """
        Delete multiple documents matching filter.
        
        Args:
            filter: MongoDB filter dictionary
        
        Returns:
            str: TOON formatted string with delete result
        """
        result = self.collection.delete_many(filter)
        
        # Return result as TOON
        result_dict = {
            "deleted_count": result.deleted_count,
            "acknowledged": result.acknowledged
        }
        return self._to_toon([result_dict])
    
    def _clean_value(self, value: Any) -> Any:
        """
        Clean a single value, handling nested structures recursively
        
        Args:
            value: Value to clean
        
        Returns:
            Cleaned value
        """
        if value is None:
            return None
        elif isinstance(value, ObjectId):
            return str(value)
        elif isinstance(value, (datetime, date, time)):
            return value.isoformat()
        elif isinstance(value, (list, tuple)):
            # Recursively clean list/tuple items
            return [self._clean_value(item) for item in value]
        elif isinstance(value, dict):
            # Recursively clean dictionary values
            return {k: self._clean_value(v) for k, v in value.items()}
        elif isinstance(value, (int, float, str, bool)):
            return value
        else:
            # Fallback for unknown types
            return str(value)
    
    def _clean_mongo_docs(self, docs: List[Dict]) -> List[Dict]:
        """
        Convert MongoDB documents to JSON-serializable format
        Uses recursive cleaning to handle nested structures
        """
        cleaned = []
        for doc in docs:
            cleaned_doc = {k: self._clean_value(v) for k, v in doc.items()}
            cleaned.append(cleaned_doc)
        return cleaned
    
    def close(self):
        """Close MongoDB connection"""
        if self.own_connection and self.collection is not None:
            self.collection.database.client.close()
            
