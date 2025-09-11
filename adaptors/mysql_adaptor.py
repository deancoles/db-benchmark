"""
mysql_adaptor.py
----------------
What this file does:
    - Connects to a MySQL server using .env credentials
    - Ensures a simple 'records' table exists
    - Provides CRUD helpers for benchmarking

Used by:
    runner.py (DB_TYPE=mysql)

Note:
    If run directly, the table is reset and demo rows are inserted
    (ignores RESET_DATA in .env; this is just a quick check).
"""

import os                         # Access environment variables
from dotenv import load_dotenv    # Load variables from .env file
import mysql.connector            # MySQL driver for Python
load_dotenv()                     # Load database credentials from .env file


# Connect to MySQL using .env variables
def connect():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE")
    )


# Create the records table if missing
def create_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS records (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255)
        )
    """)
    conn.commit()


# Insert a list of names as rows
def insert_records(conn, records):
    cur = conn.cursor()
    cur.executemany("INSERT INTO records (name) VALUES (%s)", [(r,) for r in records])
    conn.commit()


# Return all rows as (id, name)
def read_all(conn):
    cur = conn.cursor()
    cur.execute("SELECT * FROM records")
    return cur.fetchall()


# Update the 'name' for a given id
def update_record(conn, record_id, new_value):
    cur = conn.cursor()
    cur.execute("UPDATE records SET name = %s WHERE id = %s", (new_value, record_id))
    conn.commit()


# Delete the newest row (highest id)
def delete_record(conn, record_id=None):
    cur = conn.cursor()
    cur.execute("""
        DELETE FROM records
        WHERE id = (SELECT MAX(id) FROM (SELECT id FROM records) AS sub)
    """)
    conn.commit()


# Clear all rows (fresh run)
def reset_table(conn):
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE records;")
    conn.commit()


# Run a simple test if this file is executed directly
# Always resets the table, ignoring RESET_DATA flag in .env.
if __name__ == "__main__":
    conn = connect()
    create_table(conn)
    reset_table(conn)  # ensure clean state

    # Insert sample records
    insert_records(conn,["Alice", "Bob", "Charlie"])
    print("Records after insert:", read_all(conn))

    # Update id 1
    update_record(conn, 1, "Alex")
    print("Records after update:", read_all(conn))

    # Delete the newest row (highest id)
    delete_record(conn, 2)
    print("Records after delete:", read_all(conn))

    conn.close()
