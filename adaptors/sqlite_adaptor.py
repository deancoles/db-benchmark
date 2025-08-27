"""
sqlite_adaptor.py
-----------------
What this file does:
    - Opens/creates a local SQLite file
    - Ensures a simple 'records' table exists
    - Provides CRUD helpers for benchmarking

Used by:
    runner.py (DB_TYPE=sqlite)

Note:
    If run directly (python sqlite_adaptor.py), the table is always reset 
    and populated with demo records, ignoring RESET_DATA flag in .env.
"""

import sqlite3    # Built-in library to work with SQLite databases

# Open (or create) the SQLite database file
def connect(db_name="benchmark.db"):
    return sqlite3.connect(db_name)

# Create the records table if missing
def create_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT   
        )
    """)
    conn.commit()

# Insert a list of names as rows
def insert_records(conn, records):
    cur = conn.cursor()
    cur.executemany("INSERT INTO records (name) VALUES (?)", [(r,) for r in records])
    conn.commit()

# Return all rows as (id, name)
def read_all(conn):
    cur = conn.cursor()
    cur.execute("SELECT * FROM records")
    return cur.fetchall()

# Update the 'name' for a given id
def update_record(conn, record_id, new_value):
    cur = conn.cursor()
    cur.execute("UPDATE records SET name = ? WHERE id = ?", (new_value, record_id))
    conn.commit()

# Delete the row with a given id
def delete_record(conn, record_id):
    cur = conn.cursor()
    cur.execute("DELETE FROM records WHERE id = ?", (record_id,))
    conn.commit()

# Clear all rows and reset the auto-increment counter (id starts at 1).
def reset_table(conn):
    cur = conn.cursor()
    cur.execute("DELETE FROM records;")                                # remove rows
    cur.execute("DELETE FROM sqlite_sequence WHERE name='records';")   # reset counter
    conn.commit()


# Run a simple test if this file is executed directly
# Always resets the table, ignoring RESET_DATA flag in .env.
if __name__ == "__main__":
    conn = connect()
    create_table(conn)

    insert_records(conn, ["Alice", "Bob", "Charlie"])
    print("Records after insert:", read_all(conn))

    update_record(conn, 1, "Alex")
    print("Records after update:", read_all(conn))

    delete_record(conn, 2)
    print("Records after delete:", read_all(conn))

    conn.close()
