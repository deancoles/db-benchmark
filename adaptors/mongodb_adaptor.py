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
    If run directly (python mongodb_adaptor.py), the collection is always dropped 
    and demo records inserted, ignoring RESET_DATA flag in .env.
"""

import os                          # Access environment variables
from dotenv import load_dotenv     # Load variables from .env file
from pymongo import MongoClient    # MongoDB driver for Python
load_dotenv()                      # Load database connection details from .env file


# Return a handle to the target MongoDB database
def connect():
    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    db_name = os.getenv("MONGO_DATABASE", "benchmark")
    client = MongoClient(uri)
    return client[db_name]

# Insert docs with numeric seq and name
def insert_records(db, records):
    docs = [{"seq": i + 1, "name": r} for i, r in enumerate(records)]
    db.records.insert_many(docs)

# Return (seq, name) sorted by seq; omit _id for clarity
def read_all(db):
    cursor = db.records.find({}, {"_id": 0, "seq": 1, "name": 1}).sort("seq", 1)
    return [(doc["seq"], doc["name"]) for doc in cursor]

# Update by numeric seq (1-based)
def update_record(db, seq, new_name):
    db.records.update_one({"seq": seq}, {"$set": {"name": new_name}})

# Delete by numeric seq (1-based)
def delete_record(db, seq):
    db.records.delete_one({"seq": seq})


# Run a simple test if this file is executed directly
# Always resets the table, ignoring RESET_DATA flag in .env.
if __name__ == "__main__":
    db = connect()
    db.records.drop()
    db.records.create_index("seq", unique=True)        # Keep seq unique like a primary key

    # Insert sample records
    insert_records(db, ["Alice", "Bob", "Charlie"])
    print("Records after insert:", read_all(db))

    # Update seq 1
    update_record(db, 1, "Alex")
    print("Records after update:", read_all(db))

    # Delete seq 2
    delete_record(db, 2)
    print("Records after delete:", read_all(db))
