from toonpy.adapters.base import BaseAdapter
from typing import Union, Optional, Dict, Any, List
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime, date
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
    
    def _clean_mongo_docs(self, docs: List[Dict]) -> List[Dict]:
        """
        Convert MongoDB documents to JSON-serializable format
        """
        cleaned = []
        for doc in docs:
            cleaned_doc = {}
            for key, value in doc.items():
                if isinstance(value, ObjectId):
                    cleaned_doc[key] = str(value)
                elif isinstance(value, (datetime, date)):
                    cleaned_doc[key] = value.isoformat()
                else:
                    cleaned_doc[key] = value
            cleaned.append(cleaned_doc)
        return cleaned
    
    def close(self):
        """Close MongoDB connection"""
        if self.own_connection and self.collection is not None:
            self.collection.database.client.close()
            
