"""
runner.py
---------

Purpose:
    Entry point for the benchmarking tool.
    Currently connects dataset generation to SQLite adaptor
    to test end-to-end functionality.

Author: Dean Coles
Date: 2025-08-26
"""

from datasets.dataset_generator import generate_dataset
from adaptors import sqlite_adaptor


def main():
    # Generate a small dataset
    sample_data = generate_dataset(5)
    print("Generated dataset:", sample_data)

    conn = sqlite_adaptor.connect()                   # Connect to SQLite
    sqlite_adaptor.create_table(conn)                 # Ensure table exists
    sqlite_adaptor.insert_records(conn, sample_data)  # Insert dataset into table

    # Read all rows back and print
    rows = sqlite_adaptor.read_all(conn)
    print("Rows from database:", rows)

    conn.close()


# Ensures that main() only runs when this file is executed directly
if __name__ == "__main__":
    main()
    