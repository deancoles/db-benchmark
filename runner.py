"""
runner.py
---------
What this file does:
    - Chooses a database adaptor (SQLite, MySQL, MongoDB, Redis)
    - Makes a tiny dataset
    - Runs one cycle of: insert → read → update → delete
    - Prints the results and timings

Environment variables (from .env):
    DB_TYPE, RESET_DATA
    MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE
    MONGO_URI, MONGO_DATABASE
    REDIS_HOST, REDIS_PORT, REDIS_DB
"""

import os                                                  # Access environment variables
from datasets.dataset_generator import generate_dataset    # Make test dataset
from dotenv import load_dotenv                             # Load values from .env
from typing import Any, cast                               # Typing support
load_dotenv()                                              # Load connection details from .env
from utils.benchmark_utils import time_operation, summarise, print_summary_line    # Timing helpers


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

    # Make a small dataset and print DB type and dataset
    records = generate_dataset(3)                                
    print(f"DB_TYPE: {db_type}")
    print("Generated dataset:", records)

    # SQL: SQLite and MySQL branch
    if db_type in ("sqlite", "mysql"):
        conn = adaptor.connect()        # Open database 
        adaptor.create_table(conn)      # Create table if needed

        # Optionally clear table (based on RESET_DATA flag)
        if reset:
            adaptor.reset_table(conn)

        # Add records, then show data and timings
        t = time_operation(adaptor.insert_records, repeats=5, conn=conn, records=records)
        print("After insert:", adaptor.read_all(conn))
        print_summary_line(db_type, "insert", len(records), "cold" if reset else "warm", summarise(t))

        # Update record 1, then show data and timings
        t = time_operation(adaptor.update_record, repeats=5, conn=conn, record_id=1,
                           new_value=f"{records[0]} (updated)")
        print("After update:", adaptor.read_all(conn))
        print_summary_line(db_type, "update", len(records), "cold" if reset else "warm", summarise(t))

        # Remove record 2, then show data and timings
        t = time_operation(adaptor.delete_record, repeats=5, conn=conn, record_id=2)
        print("After delete:", adaptor.read_all(conn))
        print_summary_line(db_type, "delete", len(records), "cold" if reset else "warm", summarise(t))

        conn.close()    # Close database connection

    # MongoDB branch
    elif db_type == "mongodb":
        db: Any = adaptor.connect()    # Connect to MongoDB database

        # Drop collection if RESET_DATA=true
        if reset:
            db.records.drop()
            db.records.create_index("seq", unique=True)   # Add unique index on seq (acts like PK)

        # Add records, then show data and timings
        t = time_operation(adaptor.insert_records, repeats=5, db=db, records=records)
        print("After insert:", adaptor.read_all(db))
        print_summary_line(db_type, "insert", len(records), "cold" if reset else "warm", summarise(t))

        # Update record with seq=1, then show data and timings
        t = time_operation(adaptor.update_record, repeats=5, db=db, seq=1,
                           new_name=f"{records[0]} (updated)")
        print("After update:", adaptor.read_all(db))
        print_summary_line(db_type, "update", len(records), "cold" if reset else "warm", summarise(t))

        # Remove record with seq=2, then show data and timings
        t = time_operation(adaptor.delete_record, repeats=5, db=db, seq=2)
        print("After delete:", adaptor.read_all(db))
        print_summary_line(db_type, "delete", len(records), "cold" if reset else "warm", summarise(t))

    # Redis branch
    elif db_type == "redis":
        r = adaptor.connect()    # Connect to Redis server

        # Flush DB if RESET_DATA=true
        if reset:
            adaptor.reset_store(r)

        # Add records, then show data and timings
        t = time_operation(adaptor.insert_records, repeats=5, r=r, records=records)
        print("After insert:", adaptor.read_all(r))
        print_summary_line(db_type, "insert", len(records), "cold" if reset else "warm", summarise(t))

        # Update key 1, then show data and timings
        t = time_operation(adaptor.update_record, repeats=5, r=r, record_id=1,
                           new_value=f"{records[0]} (updated)")
        print("After update:", adaptor.read_all(r))
        print_summary_line(db_type, "update", len(records), "cold" if reset else "warm", summarise(t))

        # Remove key 2, then show data and timings
        t = time_operation(adaptor.delete_record, repeats=5, r=r, record_id=2)
        print("After delete:", adaptor.read_all(r))
        print_summary_line(db_type, "delete", len(records), "cold" if reset else "warm", summarise(t))


# Only run main() if this file is run directly
if __name__ == "__main__":
    main()
    