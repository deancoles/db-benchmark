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
    REPEATS, DATASET_SIZE
"""

import os                                                  # Access environment variables
from datasets.dataset_generator import generate_dataset    # Make test dataset
from dotenv import load_dotenv                             # Load values from .env
from typing import Any, cast                               # Typing support
load_dotenv()                                              # Load connection details from .env
from utils.benchmark_utils import time_operation, summarise, print_summary_line, write_summary_csv
import datetime
REPEATS = int(os.getenv("REPEATS", "5"))                   # Number of times to repeat each operation



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
        t = time_operation(adaptor.insert_records, repeats=REPEATS, conn=conn, records=records)
        print("After insert:", adaptor.read_all(conn))
        print_summary_line(db_type, "insert", len(records), "cold" if reset else "warm", summarise(t))

        # Save a daily CSV for this database + run type + dataset size.
        summary_path = f"results/summary_{datetime.datetime.now(datetime.UTC).strftime('%Y%m%d')}_{db_type}_{'cold' if reset else 'warm'}_{len(records)}.csv"
        write_summary_csv(summary_path, db_type, "insert", len(records), "cold" if reset else "warm", summarise(t))
        

        # Update record 1, then show data and timings
        t = time_operation(adaptor.update_record, repeats=REPEATS, conn=conn, record_id=1,
                           new_value=f"{records[0]} (updated)")
        print("After update:", adaptor.read_all(conn))
        print_summary_line(db_type, "update", len(records), "cold" if reset else "warm", summarise(t))
        write_summary_csv(summary_path, db_type, "update", len(records), "cold" if reset else "warm", summarise(t))

        # Remove record 2, then show data and timings
        t = time_operation(adaptor.delete_record, repeats=REPEATS, conn=conn, record_id=2)
        print("After delete:", adaptor.read_all(conn))
        print_summary_line(db_type, "delete", len(records), "cold" if reset else "warm", summarise(t))
        write_summary_csv(summary_path, db_type, "delete", len(records), "cold" if reset else "warm", summarise(t))

        conn.close()    # Close database connection

    # MongoDB branch
    elif db_type == "mongodb":
        db: Any = adaptor.connect()           # Connect to MongoDB database

        # Drop collection if RESET_DATA=true
        if reset:
            db.records.drop()
            db.records.create_index("seq")    # Non-unique index: avoids DuplicateKeyError on repeats

        # Add records, then show data and timings
        t = time_operation(adaptor.insert_records, repeats=REPEATS, db=db, records=records)
        print("After insert:", adaptor.read_all(db))
        print_summary_line(db_type, "insert", len(records), "cold" if reset else "warm", summarise(t))

        # Save a daily CSV for this database + run type + dataset size.
        summary_path = f"results/summary_{datetime.datetime.now(datetime.UTC).strftime('%Y%m%d')}_{db_type}_{'cold' if reset else 'warm'}_{len(records)}.csv"
        write_summary_csv(summary_path, db_type, "insert", len(records), "cold" if reset else "warm", summarise(t))

        # Update record with seq=1, then show data and timings
        t = time_operation(adaptor.update_record, repeats=REPEATS, db=db, seq=1,
                           new_name=f"{records[0]} (updated)")
        print("After update:", adaptor.read_all(db))
        print_summary_line(db_type, "update", len(records), "cold" if reset else "warm", summarise(t))
        write_summary_csv(summary_path, db_type, "update", len(records), "cold" if reset else "warm", summarise(t))

        # Remove record with seq=2, then show data and timings
        t = time_operation(adaptor.delete_record, repeats=REPEATS, db=db, seq=2)
        print("After delete:", adaptor.read_all(db))
        print_summary_line(db_type, "delete", len(records), "cold" if reset else "warm", summarise(t))
        write_summary_csv(summary_path, db_type, "delete", len(records), "cold" if reset else "warm", summarise(t))

    # Redis branch
    elif db_type == "redis":
        r = adaptor.connect()    # Connect to Redis server

        # Flush DB if RESET_DATA=true
        if reset:
            adaptor.reset_store(r)

        # Add records, then show data and timings
        t = time_operation(adaptor.insert_records, repeats=REPEATS, r=r, records=records)

        # Redis uses fixed keys, so repeats overwrite the same keys.
        print("After insert:", adaptor.read_all(r))
        print_summary_line(db_type, "insert", len(records), "cold" if reset else "warm", summarise(t))

        # Save a daily CSV for this database + run type + dataset size.
        summary_path = f"results/summary_{datetime.datetime.now(datetime.UTC).strftime('%Y%m%d')}_{db_type}_{'cold' if reset else 'warm'}_{len(records)}.csv"
        write_summary_csv(summary_path, db_type, "insert", len(records), "cold" if reset else "warm", summarise(t))

        # Update key 1, then show data and timings
        t = time_operation(adaptor.update_record, repeats=REPEATS, r=r, record_id=1,
                           new_value=f"{records[0]} (updated)")
        print("After update:", adaptor.read_all(r))
        print_summary_line(db_type, "update", len(records), "cold" if reset else "warm", summarise(t))
        write_summary_csv(summary_path, db_type, "update", len(records), "cold" if reset else "warm", summarise(t))

        # Remove key 2, then show data and timings
        t = time_operation(adaptor.delete_record, repeats=REPEATS, r=r, record_id=2)
        print("After delete:", adaptor.read_all(r))
        print_summary_line(db_type, "delete", len(records), "cold" if reset else "warm", summarise(t))
        write_summary_csv(summary_path, db_type, "delete", len(records), "cold" if reset else "warm", summarise(t))


# Only run main() if this file is run directly
if __name__ == "__main__":
    main()
    