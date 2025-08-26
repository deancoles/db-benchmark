"""
sqlite_adaptor.py
-----------------

Purpose:
    Provides functions to interact with an SQLite database
    for benchmarking purposes.

Details:
    - Connect to a local SQLite database file.
    - Implement basic CRUD operations (create, read, update, delete).
    - Used by runner.py to execute workloads.

Author: Dean Coles
Date: 2025-08-26
"""

import sqlite3

# Connect to the database (create if it doesn't exist)
def connect(db_name="benchmark.db"):
    return sqlite3.connect(db_name)

# Create a table for testing (if it doesn't already exist)
def create_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT   
        )
    """)
    conn.commit()

# Insert a list of records
def insert_records(conn, records):
    cur = conn.cursor()
    cur.executemany("INSERT INTO records (name) VALUES (?)", [(r,) for r in records])
    conn.commit()

# Read all rows from the table and return them
def read_all(conn):
    cur = conn.cursor()
    cur.execute("SELECT * FROM records")
    return cur.fetchall()

# Update a record's name field by id
def update_record(conn, record_id, new_value):
    cur = conn.cursor()
    cur.execute("UPDATE records SET name = ? WHERE id = ?", (new_value, record_id))
    conn.commit()

# Delete a record by id
def delete_record(conn, record_id):
    cur = conn.cursor()
    cur.execute("DELETE FROM records WHERE id = ?", (record_id,))


# Test block to check CRUD functions
if __name__ == "__main__":
    conn = connect()
    create_table(conn)

    # Insert sample records
    insert_records(conn, ["Alice", "Bob", "Charlie"])
    print("Records after insert:", read_all(conn))

    # Update id 1
    update_record(conn, 1, "Alex")
    print("Records after update:", read_all(conn))

    # Delete id 2
    delete_record(conn, 2)
    print("Records after delete:", read_all(conn))

    conn.close()
