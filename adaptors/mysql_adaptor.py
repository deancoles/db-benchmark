"""
mysql_adaptor.py
----------------

Purpose:
    Provides functions to interact with a MySQL database
    for benchmarking purposes.

Details:
    - Connect to a MySQL database server.
    - Implements basic CRUD operations (create, read, update, delete).
    - Used by runner.py to execute workloads.

Author: Dean Coles
Date: 2025-08-26
"""

import os                         # Access environment variables
from dotenv import load_dotenv    # Load variables from .env file
import mysql.connector            # MySQL driver for Python
load_dotenv()                     # Load database credentials from .env file


# Connect to MySQL database
def connect():
    """Connect to the MySQL database using environment variables."""
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE")
    )

# Create a table for testing (if it does not already exist)
def create_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS records (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255)
        )
    """)
    conn.commit()

# Insert a list of records into the table
def insert_records(conn, records):
    cur = conn.cursor()
    cur.executemany("INSERT INTO records (name) VALUES (%s)", [(r,) for r in records])
    conn.commit()

# Read all rows from the table and return them
def read_all(conn):
    cur = conn.cursor()
    cur.execute("SELECT * FROM records")
    return cur.fetchall()

# Update a record's name field by id
def update_record(conn, record_id, new_value):
    cur = conn.cursor()
    cur.execute("UPDATE records SET name = %s WHERE id = %s", (new_value, record_id))
    conn.commit()

# Delete a record by id
def delete_record(conn, record_id):
    cur = conn.cursor()
    cur.execute("DELETE FROM records WHERE id = %s", (record_id,))
    conn.commit()

# Remove all rows so each run starts clean
def reset_table(conn):
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE records;")
    conn.commit()


# Test block to check CRUD functions
if __name__ == "__main__":
    conn = connect()
    create_table(conn)

    # Insert sample records
    insert_records(conn,["Alice", "Bob", "Charlie"])
    print("Records after insert", read_all(conn))

    # Update id 1
    update_record(conn, 1, "Alex")
    print("Records after update:", read_all(conn))

    # Delete id 2
    delete_record(conn, 2)
    print("Records after delete:", read_all(conn))

    conn.close()
