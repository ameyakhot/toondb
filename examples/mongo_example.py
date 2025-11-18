from toondb import MongoAdapter, to_toon
from pymongo import MongoClient

MONGO_URI = "XXX"
MONGO_DATABASE = "XXX"
MONGO_COLLECTION = "XXX"

client = MongoClient(MONGO_URI)
collection = client[MONGO_DATABASE].MONGO_COLLECTION  # Use the places collection

adapter = MongoAdapter(collection=collection)

# Query: add your mongo query in the object below
query = {}
projection = None  # Get all fields, or specify fields you want

# Manually limit the number of results using pymongo cursor, since our adapter does not have limit support
results_cursor = collection.find(query, projection).limit(10)
results = list(results_cursor)

# TOON encode the results
toon_result = adapter._to_toon(results)

print(toon_result)