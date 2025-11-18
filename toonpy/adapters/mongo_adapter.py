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
            
