"""
mongodb_adaptor.py
------------------

Purpose:
    Provides functions to interact with a MongoDB database
    for benchmarking purposes.

Details:
    - Connect to a MongoDB server.
    - Implements basic CRUD operations (create, read, update, delete).
    - Used by runner.py to execute workloads.

Author: Dean Coles
Date: 2025-08-26
"""

import os                          # Access environment variables
from dotenv import load_dotenv     # Load variables from .env file
from pymongo import MongoClient    # MongoDB driver for Python
load_dotenv()                      # Load database connection details from .env file

# Connect to the MongoDB database
def connect():
    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    db_name = os.getenv("MONGO_DATABASE", "benchmark")
    client = MongoClient(uri)
    return client[db_name]

# Insert a list of records
def insert_records(db, records):
    db.records.insert_many([{"name": r} for r in records])

# Read all documents from the collection
def read_all(db):
    return list(db.records.find({}, {"_id": 0}))

# Update a record by matching its name
def update_record(db, old_name, new_name):
    db.records.update_one({"name": old_name}, {"$set": {"name": new_name}})

# Delete a record by matching its name
def delete_record(db, name):
    db.records.delete_one({"name": name})

# Test block to check CRUD functions
if __name__ == "__main__":
    db = connect()
    db.records.drop()

    # Insert sample records
    insert_records(db, ["Alice", "Bob", "Charlie"])
    print("Records after insert:", read_all(db))

    # Update id 1
    update_record(db, "Alice", "Alex")
    print("Records after update:", read_all(db))

    # Delete id 2
    delete_record(db, "Bob")
    print("Records after delete:", read_all(db))
