"""
mongodb_adaptor.py
------------------
What this file does:
    - Connects to MongoDB (via URI in .env)
    - Uses a 'records' collection
    - Provides CRUD helpers for benchmarking

Used by:
    runner.py (DB_TYPE=mongodb)

Note:
    If run directly, the collection is dropped and demo docs are inserted
    (ignores RESET_DATA in .env; this is just a quick check).
"""

import os                          # Access environment variables
import re
from dotenv import load_dotenv     # Load variables from .env file
from pymongo import MongoClient    # MongoDB driver for Python
load_dotenv()                      # Load database connection details from .env file


# Connect to MongoDB and return a database handle
def connect():
    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    db_name = os.getenv("MONGO_DATABASE", "benchmark")
    client = MongoClient(uri)
    return client[db_name]


# Insert docs with numeric seq and name
def insert_records(db, records):
    docs = [{"seq": i + 1, "name": r} for i, r in enumerate(records)]               # Build docs: [{"seq": 1, "name": "Alice"}, ...]
    db.records.insert_many(docs)                                                    # Insert all documents into 'records' collection


# Return (seq, name) sorted by seq (omit _id for clarity)
def read_all(db):
    cursor = db.records.find({}, {"_id": 0, "seq": 1, "name": 1}).sort("seq", 1)    # Query all docs, exclude _id, include seq and name, sort ascending by seq
    return [(doc["seq"], doc["name"]) for doc in cursor]                            # Build list of (seq, name) tuples


# Update one document by seq
def update_record(db, seq, new_name):
    db.records.update_one({"seq": seq}, {"$set": {"name": new_name}})               # Find doc with given seq and update name field


# Delete the newest document (highest seq)
def delete_record(db, record_id=None):
    db.records.find_one_and_delete({}, sort=[("seq", -1)])                          # Find one doc with highest seq and delete it       


 
def read_by_id(db, seq: int):
    return db.records.find_one({"seq": seq}, {"_id": 0, "seq": 1, "name": 1})


def filter_contains(db, substring: str):
    pattern = re.escape(substring)
    cursor = db.records.find(
        {"name": {"$regex": pattern}},
        {"_id": 0, "seq": 1, "name": 1},
    ).sort("seq", 1)
    return [(doc["seq"], doc["name"]) for doc in cursor]


# Run a simple test if this file is executed directly
# Always resets the collection, ignoring RESET_DATA flag in .env.
if __name__ == "__main__":
    db = connect()                                                                  # Connect to MongoDB
    db.records.drop()                                                               # Drop collection for a clean start
    db.records.create_index("seq")                                                  # Non-unique index so repeats won't fail

    # Insert sample records
    insert_records(db, ["Alice", "Bob", "Charlie"])
    print("Records after insert:", read_all(db))

    # Update seq 1
    update_record(db, 1, "Alex")
    print("Records after update:", read_all(db))

    # Delete the newest document (highest seq)
    delete_record(db, 2)
    print("Records after delete:", read_all(db))
    