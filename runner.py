"""
runner.py
---------

Purpose:
    Entry point for the benchmarking tool.
    Selects a database adaptor based on configuration and runs a simple workload.

Details:
    - Reads DB_TYPE from environment variables (sqlite / mysql / mongodb).
    - Generates a small dataset and runs insert, read, update, delete.
    - Prints rows after each operation for quick verification.

Author: Dean Coles
Date: 2025-08-26
"""

import os                                                  # Access environment variables
from datasets.dataset_generator import generate_dataset    # Generate sample records
from dotenv import load_dotenv                             # Load variables from .env file
from typing import Any, cast
load_dotenv()                                              # Load connection details from .env


# Returns chosen adaptor module
def select_adaptor(db_type):
    if db_type == "sqlite":
        from adaptors import sqlite_adaptor as adaptor
    elif db_type == "mysql":
        from adaptors import mysql_adaptor as adaptor
    elif db_type == "mongodb":
        from adaptors import mongodb_adaptor as adaptor
    else:
        raise ValueError(f"Unsupported DB_TYPE '{db_type}'. Use sqlite, mysql, or mongodb.")
    return adaptor


#
def main():
    #Read target database type
    db_type = os.getenv("DB_TYPE","sqlite").lower()
    adaptor = cast(Any, select_adaptor(db_type))

    # Generate a small dataset
    records = generate_dataset(3)
    print(f"DB_TYPE: {db_type}")
    print("Generated dataset:", records)

    reset = os.getenv("RESET_DATA", "true").lower() == "true"    # Check reset flag from .env

    if db_type in ("sqlite", "mysql"):
        conn = adaptor.connect()
        adaptor.create_table(conn)

        # Optionally reset table (based on RESET_DATA flag)
        reset = os.getenv("RESET_DATA", "true").lower() == "true"
        if reset:
            adaptor.reset_table(conn)

        # Insert all records
        adaptor.insert_records(conn, records)
        print("After insert:", adaptor.read_all(conn))

        # Update record with id 1
        adaptor.update_record(conn, 1, f"{records[0]} (updated)")
        print("After update:", adaptor.read_all(conn))

        # Delete record with id 2
        adaptor.delete_record(conn, 2)
        print("After delete:", adaptor.read_all(conn))

        # Close the connection
        conn.close()

    elif db_type == "mongodb":
        db: Any = adaptor.connect()

        # If reset flag enabled, clear data
        if reset:
            db.records.drop()

        # Insert all records
        adaptor.insert_records(db, records)
        print("After insert:", adaptor.read_all(db))

        # Update a record by name
        adaptor.update_record(db, records[0], f"{records[0]} (updated)")
        print("After update:", adaptor.read_all(db))

        # Delete a record by name
        adaptor.delete_record(db, records[1])
        print("After delete:", adaptor.read_all(db))


# Ensures that main() only runs when this file is executed directly
if __name__ == "__main__":
    main()
    