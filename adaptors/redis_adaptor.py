"""
redis_adaptor.py
----------------
What this file does:
    - Connects to Memurai/Redis on localhost:6379
    - Stores values under keys like 'record:1'
    - Provides CRUD helpers for benchmarking (key-value)

Used by:
    runner.py (DB_TYPE=redis)

Note:
    If run directly (python redis_adaptor.py), the Redis database is always flushed 
    and demo records inserted, ignoring RESET_DATA flag in .env.
"""

import os                         # Access environment variables
from dotenv import load_dotenv    # Load variables from .env file
import redis                      # Redis (and Memurai) client
load_dotenv()                     # Load database connection details from .env file


# Create a Redis client using .env variables
def connect():
    return redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=int(os.getenv("REDIS_DB", "0")),
        decode_responses=True                         # Return str instead of bytes
    )

# Clear all keys in the selected logical DB (fresh run)
def reset_store(r):
    r.flushdb()

# Consistent key naming for ids
def _key(i: int) -> str:
    return f"record:{i}"

# Insert values as record:1..N.
def insert_records(r, records):
    pipe = r.pipeline()                           # Pipeline for batched writes
    for i, val in enumerate(records, start=1):    # 1-based ids
        pipe.set(_key(i), val)                    # Set key -> value
    pipe.execute()

# Return all keys as (id, value) sorted by id
def read_all(r):
    keys =r.keys("record:*")                                   # Fetch matching keys
    keys = sorted(keys, key=lambda k: int(k.split(":")[1]))    # Sort by numeric id
    return [(int(k.split(":")[1]), r.get(k)) for k in keys]    # Return (id, value)

# Overwrite value for a given id
def update_record(r, record_id, new_value):
    r.set(_key(record_id), new_value)

# Delete the key for a given id
def delete_record(r, record_id):
    r.delete(_key(record_id))

# Run a simple test if this file is executed directly
# Always resets the table, ignoring RESET_DATA flag in .env.
if __name__ == "__main__":
    client = connect()
    reset_store(client)

    insert_records(client,["Alice", "Bob", "Charlie"])
    print("After insert:", read_all(client))

    update_record(client, 1, "Alex")
    print("After update:", read_all(client))

    delete_record(client, 2)
    print("After delete:", read_all(client))
