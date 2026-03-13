"""
mongodb_adaptor.py
------------------
What this file does:
    - Connects to MongoDB (via URI in .env)
    - Uses a 'records' collection
    - Provides CRUD helpers for benchmarking

Used by:
    - runner.py (DB_TYPE=mongodb)
"""

import os                           # Access environment variables
import re                           # Escape substring patterns for regex searches
from dotenv import load_dotenv      # Load variables from .env file
from pymongo import MongoClient     # MongoDB client driver for Python
from pymongo import ReturnDocument  # Return updated counter document after increment
load_dotenv()                       # Load database connection details from .env file

SEQ_DOC_ID = "record_seq"           # fixed id for the counter document


# Connect to MongoDB and return a database handle
def connect():
    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    db_name = os.getenv("MONGO_DATABASE", "benchmark")
    client = MongoClient(uri)
    return client[db_name]


# Generate the next sequence values for inserted records
def _next_seqs(db, n: int):
    doc = db.meta.find_one_and_update(
        {"_id": SEQ_DOC_ID},
        {"$inc": {"value": n}},
        upsert=True,
        return_document=ReturnDocument.AFTER
    )
    end_value = int(doc["value"])
    start_value = end_value - n + 1
    return list(range(start_value, end_value + 1))


# Insert multiple records using sequential IDs
def insert_records(db, records):
    seqs = _next_seqs(db, len(records))
    docs = [{"seq": s, "name": r} for s, r in zip(seqs, records)]   # Build one document per record using seq as the benchmark id field
    db.records.insert_many(docs)


# Query all records and return seq and name sorted by seq
def read_all(db):
    cursor = db.records.find({}, {"_id": 0, "seq": 1, "name": 1}).sort("seq", 1)    
    return [(doc["seq"], doc["name"]) for doc in cursor]                            


# Return one document by seq (omit _id for clarity)
def read_by_id(db, seq):
    doc = db.records.find_one({"seq": seq}, {"_id": 0, "seq": 1, "name": 1})
    if not doc:
        return None
    return (doc["seq"], doc["name"])


# Update one document by seq
def update_record(db, seq, new_name):
    db.records.update_one({"seq": seq}, {"$set": {"name": new_name}})               


# Delete by seq if provided, otherwise delete newest (highest seq)
def delete_record(db, seq=None):
    if seq is not None:
        db.records.delete_one({"seq": seq})
        return
    db.records.find_one_and_delete({}, sort=[("seq", -1)])


# Return records where the name field contains a substring
def filter_contains(db, substring: str):
    pattern = re.escape(substring)
    cursor = db.records.find(
        {"name": {"$regex": pattern}},
        {"_id": 0, "seq": 1, "name": 1},
    ).sort("seq", 1)
    return [(doc["seq"], doc["name"]) for doc in cursor]


# Return total number of records in the collection
def count_records(db):
    return db.records.count_documents({})


# Run a simple test if this file is executed directly
if __name__ == "__main__":
    db = connect()                                                                  
    db.records.drop()                                                               
    db.meta.delete_one({"_id": SEQ_DOC_ID})                                         
    db.records.create_index("seq", unique=True)                                     

    # Insert three sample records and display the result
    insert_records(db, ["Alice", "Bob", "Charlie"])
    print("Records after insert:", read_all(db))

    # Update the first record and show the updated dataset
    update_record(db, 1, "Alex")
    print("Records after update:", read_all(db))

    # Delete the record with seq 2 and display the remaining data
    delete_record(db, 2)
    print("Records after delete:", read_all(db))
    