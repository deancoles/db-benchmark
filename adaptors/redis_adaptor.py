"""
redis_adaptor.py
----------------
What this file does:
    - Connects to Redis/Memurai on localhost
    - Stores values under keys like 'record:1'
    - Uses a Redis counter 'record:seq' so warm runs accumulate (like SQL/Mongo)
    - Provides CRUD helpers for benchmarking

Used by:
    runner.py (DB_TYPE=redis)

Note:
    If run directly, the DB is flushed and demo keys are inserted
    (ignores RESET_DATA in .env; this is just a quick check).
"""

import os                                                    # Access environment variables
from dotenv import load_dotenv                               # Load variables from .env file
import redis                                                 # Redis (and Memurai) client
load_dotenv()                                                # Load database connection details from .env file


# Connect to Redis using host/port/db from .env (defaults if missing)
def connect():
    return redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=int(os.getenv("REDIS_DB", "0")),
        decode_responses=True                                # Return str instead of bytes 
    )


# Clear all keys (fresh start for cold runs)
def reset_store(r):
    r.flushdb()


# Build a key name in the form "record:<id>"
def _key(i: int) -> str:
    return f"record:{i}"


# Special Redis key used as a counter for unique ids
SEQ_KEY = "record:seq"


# Convert Redis bytes into str (safety if decode_responses=False)
def _as_str(b):
    return b.decode() if isinstance(b, (bytes, bytearray)) else b


# Yield numeric ids only (ignore 'record:seq' and any non-numeric keys)
def _numeric_ids(r):

    # Loop all keys starting with 'record:'
    for k in r.scan_iter("record:*"):
        s = _as_str(k)                                      # Ensure key is a string
        parts = s.split(":")                                # Split into 'record', '<id>'

        # Check suffix is numeric
        if len(parts) == 2 and parts[1].isdigit():
            yield int(parts[1])                             # Yield numeric id


# Insert multiple values with increasing ids
def insert_records(r, records):

    # Initialise the sequence counter if it doesn't exist
    if not r.exists(SEQ_KEY):
        existing_ids = list(_numeric_ids(r))                # Collect existing numeric ids
        max_id = max(existing_ids) if existing_ids else 0   # Highest id so far (or 0)
        r.set(SEQ_KEY, max_id)                              # Set counter to current max

    pipe = r.pipeline()                                     # Pipeline batches Redis commands
    for _ in records:
        pipe.incr(SEQ_KEY)                      
    new_ids = pipe.execute()   

    pipe = r.pipeline()
    for new_id, val in zip(new_ids, records):
        pipe.set(_key(new_id), val)
    pipe.execute()                    


# Return all rows as list of (id, value) pairs
def read_all(r):
    ids = sorted(_numeric_ids(r))                           # Sorted list of numeric ids
    out = []                                                # Collect results
    
    # Loop through all ids in order
    for i in ids:
        v = r.get(_key(i))                                  # Fetch value for each id
        v = _as_str(v) if v is not None else None           # Convert to str if needed
        out.append((i, v))                                  # Add (id, value) tuple
    
    return out


# Return one row by id (key lookup)
def read_by_id(r, record_id):
    v = r.get(_key(record_id))
    if v is None:
        return None
    v = _as_str(v)
    return (record_id, v)


# Update one value by id
def update_record(r, record_id, new_value):
    r.set(_key(record_id), new_value)                      # Replace value at key


# Delete by id if provided, otherwise delete newest (highest id)
def delete_record(r, record_id=None):

    # Delete a specific id if requested
    if record_id is not None:
        r.delete(_key(record_id))
        return

    # Otherwise delete newest
    ids = list(_numeric_ids(r))
    if not ids:
        return
    r.delete(_key(max(ids)))


def filter_contains(r, substring: str):
    out = []
    for i in sorted(_numeric_ids(r)):
        v = r.get(_key(i))
        v = _as_str(v) if v is not None else ""
        if substring in v:
            out.append((i, v))
    return out


# Run a simple test if this file is executed directly
# Always resets the store, ignoring RESET_DATA flag in .env.
if __name__ == "__main__":
    client = connect()                                     # Connect to Redis
    reset_store(client)                                    # Ensure reset

    insert_records(client,["Alice", "Bob", "Charlie"])
    print("After insert:", read_all(client))

    update_record(client, 1, "Alex")
    print("After update:", read_all(client))

    delete_record(client, 2)
    print("After delete:", read_all(client))
    