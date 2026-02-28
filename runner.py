"""
runner.py
---------
Runs a benchmark cycle for the chosen database.

What this script does:
  - Builds a small dataset of names (size from .env)
  - Runs INSERT, UPDATE, DELETE with timing + repeats
  - Prints short summaries (3 d.p.) and writes a CSV row per op

Controlled by .env:
  DB_TYPE       sqlite | mysql | mongodb | redis
  RESET_DATA    true=cold (fresh) / false=warm (keep data)
  DATASET_SIZE  how many names to generate
  REPEATS       repeats per timed operation
  OUTPUT_DETAIL count | preview | full (how much to print after each op)
"""

import os                                                  # Access environment variables
from datasets.dataset_generator import generate_dataset    # Make test dataset
from dotenv import load_dotenv                             # Load values from .env
from typing import Any, cast                               # Typing support
load_dotenv()                                              # Load connection details from .env
from utils.benchmark_utils import time_operation, summarise, print_summary_line, write_summary_csv
import datetime
REPEATS = int(os.getenv("REPEATS", "5"))                   # Number of times to repeat each operation
FILTER_TERM = os.getenv("FILTER_TERM", "Record 1")
READ_ID = int(os.getenv("READ_ID", "1"))

# Shorten long lists for printing (first 5, "...", last 5)
def preview_list(items, head: int = 5, tail: int = 5):
    items = list(items)
    if len(items) <= head + tail:
        return items
    return items[:head] + ["..."] + items[-tail:]

# Choose how much to print after each operation: count / preview / full
OUTPUT_DETAIL = os.getenv("OUTPUT_DETAIL", "count").lower()
def display_after(items):
    items = list(items)
    if OUTPUT_DETAIL == "count":
        return f"{len(items)} items"
    elif OUTPUT_DETAIL == "preview":
        return preview_list(items)
    else:
        return items

# Decide which adaptor to use
def select_adaptor(db_type):
    if db_type == "sqlite":
        from adaptors import sqlite_adaptor as adaptor
    elif db_type == "mysql":
        from adaptors import mysql_adaptor as adaptor
    elif db_type == "mongodb":
        from adaptors import mongodb_adaptor as adaptor
    elif db_type == "redis":
        from adaptors import redis_adaptor as adaptor
    else:
        raise ValueError(f"Unsupported DB_TYPE '{db_type}'. Use sqlite, mysql, mongodb, or redis.")
    return adaptor    


# Run one CRUD cycle (insert → read → update → delete) for the selected database
def main():
    db_type = os.getenv("DB_TYPE","sqlite").lower()              # DB to test (sqlite default)
    reset = os.getenv("RESET_DATA", "true").lower() == "true"    # Check reset flag from .env
    adaptor = cast(Any, select_adaptor(db_type))                 # Load chosen adaptor

    # Make a dataset (size comes from .env, default 3)
    DATASET_SIZE = int(os.getenv("DATASET_SIZE", "3"))
    records = generate_dataset(DATASET_SIZE)
    print(f"\nDB_TYPE: {db_type}")

    # Print a preview of the dataset (first 5 and last 5 records if large)
    if len(records) > 10:
        preview = records[:5] + ["..."] + records[-5:]
    else:
        preview = records
    print("Generated dataset:", preview)

    # SQL: SQLite and MySQL branch. Connect, reset if needed, then run insert/update/delete
    if db_type in ("sqlite", "mysql"):
        conn = adaptor.connect()        # Open database 
        adaptor.create_table(conn)      # Create table if needed

        # Optionally clear table (based on RESET_DATA flag)
        if reset:
            adaptor.reset_table(conn)

        # Add records, then show data and timings
        t = time_operation(adaptor.insert_records, repeats=REPEATS, conn=conn, records=records)
        print("\nTable Size After INSERT:", display_after(adaptor.read_all(conn)))
        print_summary_line(db_type, "insert", len(records), "cold" if reset else "warm", summarise(t))

        # Save a daily CSV for this database + run type + dataset size.
        summary_path = f"results/summary_{datetime.datetime.now(datetime.UTC).strftime('%Y%m%d')}_{db_type}_{'cold' if reset else 'warm'}_{len(records)}.csv"
        write_summary_csv(summary_path, db_type, "insert", len(records), "cold" if reset else "warm", summarise(t))

        # Time READ_ALL
        t = time_operation(adaptor.read_all, repeats=REPEATS, conn=conn)
        print("\nFull Scan returned:", display_after(adaptor.read_all(conn)))
        print_summary_line(db_type, "full_scan", len(records), "cold" if reset else "warm", summarise(t))
        write_summary_csv(summary_path, db_type, "full_scan", len(records), "cold" if reset else "warm", summarise(t))

        # Time READ_BY_ID (primary key lookup)
        t = time_operation(adaptor.read_by_id, repeats=REPEATS, conn=conn, record_id=READ_ID)
        result = adaptor.read_by_id(conn, READ_ID)
        print(f"\nPrimary Key Lookup returned (id={READ_ID}):", result)
        print_summary_line(db_type, "primary_key_lookup", len(records), "cold" if reset else "warm", summarise(t))
        write_summary_csv(summary_path, db_type, "primary_key_lookup", len(records), "cold" if reset else "warm", summarise(t))

        # Update record 1, then show data and timings
        t = time_operation(adaptor.update_record, repeats=REPEATS, conn=conn, record_id=1,
                           new_value=f"{records[0]} (updated)")
        print("\nTable Size After UPDATE:", display_after(adaptor.read_all(conn)))
        print_summary_line(db_type, "update", len(records), "cold" if reset else "warm", summarise(t))
        write_summary_csv(summary_path, db_type, "update", len(records), "cold" if reset else "warm", summarise(t))

        # Remove record 2, then show data and timings
        t = time_operation(adaptor.delete_record, repeats=REPEATS, conn=conn, record_id=2)
        print("\nTable Size After DELETE:", display_after(adaptor.read_all(conn)))
        print_summary_line(db_type, "delete", len(records), "cold" if reset else "warm", summarise(t))
        write_summary_csv(summary_path, db_type, "delete", len(records), "cold" if reset else "warm", summarise(t))

        conn.close()    # Close database connection

    # MongoDB branch. Connect, reset if needed, then run insert/update/delete
    elif db_type == "mongodb":
        db: Any = adaptor.connect()           # Connect to MongoDB database

        # Drop collection if RESET_DATA=true
        if reset:
            db.records.drop()
            db.records.create_index("seq")    # Non-unique index: avoids DuplicateKeyError on repeats

        # Add records, then show data and timings
        t = time_operation(adaptor.insert_records, repeats=REPEATS, db=db, records=records)
        print("\nTable Size After INSERT:", display_after(adaptor.read_all(db)))
        print_summary_line(db_type, "insert", len(records), "cold" if reset else "warm", summarise(t))

        # Save a daily CSV for this database + run type + dataset size.
        summary_path = f"results/summary_{datetime.datetime.now(datetime.UTC).strftime('%Y%m%d')}_{db_type}_{'cold' if reset else 'warm'}_{len(records)}.csv"
        write_summary_csv(summary_path, db_type, "insert", len(records), "cold" if reset else "warm", summarise(t))

        # Time READ_ALL
        t = time_operation(adaptor.read_all, repeats=REPEATS, db=db)
        print("\nFull Scan returned:", display_after(adaptor.read_all(db)))
        print_summary_line(db_type, "full_scan", len(records), "cold" if reset else "warm", summarise(t))
        write_summary_csv(summary_path, db_type, "full_scan", len(records), "cold" if reset else "warm", summarise(t))

        # Time READ_BY_ID (lookup by seq)
        t = time_operation(adaptor.read_by_id, repeats=REPEATS, db=db, seq=READ_ID)
        result = adaptor.read_by_id(db, READ_ID)
        print(f"\nPrimary Key Lookup returned (id={READ_ID}):", result)
        print_summary_line(db_type, "primary_key_lookup", len(records), "cold" if reset else "warm", summarise(t))
        write_summary_csv(summary_path, db_type, "primary_key_lookup", len(records), "cold" if reset else "warm", summarise(t))

        # Update record with seq=1, then show data and timings
        t = time_operation(adaptor.update_record, repeats=REPEATS, db=db, seq=1, new_name=f"{records[0]} (updated)")
        print("\nTable Size After UPDATE:", display_after(adaptor.read_all(db)))
        print_summary_line(db_type, "update", len(records), "cold" if reset else "warm", summarise(t))
        write_summary_csv(summary_path, db_type, "update", len(records), "cold" if reset else "warm", summarise(t))

        # Remove record with seq=2, then show data and timings
        t = time_operation(adaptor.delete_record, repeats=REPEATS, db=db)
        print("\nTable Size After DELETE:", display_after(adaptor.read_all(db)))
        print_summary_line(db_type, "delete", len(records), "cold" if reset else "warm", summarise(t))
        write_summary_csv(summary_path, db_type, "delete", len(records), "cold" if reset else "warm", summarise(t))

    # Redis branch. Connect, reset if needed, then run insert/update/delete
    elif db_type == "redis":
        r = adaptor.connect()    # Connect to Redis server

        # Flush DB if RESET_DATA=true
        if reset:
            adaptor.reset_store(r)

        # Add records, then show data and timings
        t = time_operation(adaptor.insert_records, repeats=REPEATS, r=r, records=records)

        # Redis uses an internal counter so warm runs accumulate like SQL/Mongo
        print("\nTable Size After INSERT:", display_after(adaptor.read_all(r)))
        print_summary_line(db_type, "insert", len(records), "cold" if reset else "warm", summarise(t))

        # Save a daily CSV for this database + run type + dataset size.
        summary_path = f"results/summary_{datetime.datetime.now(datetime.UTC).strftime('%Y%m%d')}_{db_type}_{'cold' if reset else 'warm'}_{len(records)}.csv"
        write_summary_csv(summary_path, db_type, "insert", len(records), "cold" if reset else "warm", summarise(t))

        # Time READ_ALL
        t = time_operation(adaptor.read_all, repeats=REPEATS, r=r)
        print("\nFull Scan returned:", display_after(adaptor.read_all(r)))
        print_summary_line(db_type, "full_scan", len(records), "cold" if reset else "warm", summarise(t))
        write_summary_csv(summary_path, db_type, "full_scan", len(records), "cold" if reset else "warm", summarise(t))

        # Time READ_BY_ID (key lookup)
        t = time_operation(adaptor.read_by_id, repeats=REPEATS, r=r, record_id=READ_ID)
        result = adaptor.read_by_id(r, READ_ID)
        print(f"\nPrimary Key Lookup returned (id={READ_ID}):", result)
        print_summary_line(db_type, "primary_key_lookup", len(records), "cold" if reset else "warm", summarise(t))
        write_summary_csv(summary_path, db_type, "primary_key_lookup", len(records), "cold" if reset else "warm", summarise(t))

        # Update key 1, then show data and timings
        t = time_operation(adaptor.update_record, repeats=REPEATS, r=r, record_id=1, new_value=f"{records[0]} (updated)")
        print("\nTable Size After UPDATE:", display_after(adaptor.read_all(r)))
        print_summary_line(db_type, "update", len(records), "cold" if reset else "warm", summarise(t))
        write_summary_csv(summary_path, db_type, "update", len(records), "cold" if reset else "warm", summarise(t))

        # Remove key 2, then show data and timings
        t = time_operation(adaptor.delete_record, repeats=REPEATS, r=r, record_id=2)
        print("\nTable Size After DELETE:", display_after(adaptor.read_all(r)))
        print_summary_line(db_type, "delete", len(records), "cold" if reset else "warm", summarise(t))
        write_summary_csv(summary_path, db_type, "delete", len(records), "cold" if reset else "warm", summarise(t))


# Only run main() if this file is run directly
if __name__ == "__main__":
    main()
    print()  # Adds a blank line before returning to terminal
