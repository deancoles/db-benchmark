"""
runner.py
---------
What this file does:
    - Picks a database adaptor based on DB_TYPE in .env
    - Makes a tiny dataset
    - Runs one CRUD cycle: insert → read → update → delete
    - Prints results so you can see it worked

Supported DB_TYPE values:
    sqlite, mysql, mongodb, redis

Environment variables (loaded via .env):
    General:   DB_TYPE, RESET_DATA
    MySQL:     MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE
    MongoDB:   MONGO_URI, MONGO_DATABASE
    Redis:     REDIS_HOST, REDIS_PORT, REDIS_DB

Note:
    This run is a functional sanity check. Real benchmarks will scale the dataset
    and time operations separately.
"""

import os                                                  # Access environment variables
from datasets.dataset_generator import generate_dataset    # Generate sample records
from dotenv import load_dotenv                             # Load variables from .env file
from typing import Any, cast                               # Help with typing for adaptors
load_dotenv()                                              # Load connection details from .env


# Pick and return the correct adaptor module
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
    
    db_type = os.getenv("DB_TYPE","sqlite").lower()              # Database to test (sqlite default)
    reset = os.getenv("RESET_DATA", "true").lower() == "true"    # Check reset flag from .env
    adaptor = cast(Any, select_adaptor(db_type))

    # Small dataset for quick verification (benchmarks will use larger sizes later)
    records = generate_dataset(3)
    print(f"DB_TYPE: {db_type}")
    print("Generated dataset:", records)

    # SQL: connect → ensure table → optional reset → CRUD
    if db_type in ("sqlite", "mysql"):
        conn = adaptor.connect()
        adaptor.create_table(conn)

        # Optionally reset table (based on RESET_DATA flag)
        if reset:
            adaptor.reset_table(conn)

        adaptor.insert_records(conn, records)
        print("After insert:", adaptor.read_all(conn))

        adaptor.update_record(conn, 1, f"{records[0]} (updated)")
        print("After update:", adaptor.read_all(conn))

        adaptor.delete_record(conn, 2)
        print("After delete:", adaptor.read_all(conn))

        conn.close()

    # MongoDB: connect → optional reset → CRUD
    elif db_type == "mongodb":
        db: Any = adaptor.connect()

        # Drop the collection for a clean start if requested
        if reset:
            db.records.drop()
            db.records.create_index("seq", unique=True)   # Ensure seq behaves like a primary key

        adaptor.insert_records(db, records)
        print("After insert:", adaptor.read_all(db))

        adaptor.update_record(db, 1, f"{records[0]} (updated)")
        print("After update:", adaptor.read_all(db))

        adaptor.delete_record(db, 2)
        print("After delete:", adaptor.read_all(db))

    # Redis: connect → optional reset → CRUD
    elif db_type == "redis":
        r = adaptor.connect()

        # Flush the logical DB for a clean start if requested
        if reset:
            adaptor.reset_store(r)

        adaptor.insert_records(r, records)
        print("After insert:", adaptor.read_all(r))

        adaptor.update_record(r, 1, f"{records[0]} (updated)")
        print("After update:", adaptor.read_all(r))

        adaptor.delete_record(r, 2)
        print("After delete:", adaptor.read_all(r))


# Ensures that main() only runs when this file is executed directly
if __name__ == "__main__":
    main()
    