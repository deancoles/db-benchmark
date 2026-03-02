"""
runner.py
---------
Runs benchmarking for the chosen database adaptor.

What this script does:
  - Builds a dataset of names (size from .env)
  - Scenario mode: runs INSERT → FULL_SCAN → PRIMARY_KEY_LOOKUP → UPDATE → DELETE
  - Isolated mode: runs ONE chosen operation on a reset baseline dataset
  - Prints summaries (3 d.p.) and writes a CSV row per operation

Controlled by .env:
  DB_TYPE       sqlite | mysql | mongodb | redis
  RESET_DATA    Scenario mode only: true=cold (clear data) / false=warm (retain data)
  DATASET_SIZE  number of records to generate per run
  REPEATS       repeats per timed operation
  ISOLATED_MODE true/false (run one operation in isolation)
  ISOLATED_OP   insert | full_scan | primary_key_lookup | update | delete
  READ_ID       record/key/seq to use for lookups
  OUTPUT_DETAIL count | preview | full (how much to print after each op)
"""

import os                                                   # Access environment variables
import datetime                                             # Timestamped CSV filenames
from datasets.dataset_generator import generate_dataset     # Make test dataset
from dotenv import load_dotenv                              # Load values from .env
from typing import Any, cast                                # Typing support
from utils.benchmark_utils import (
    time_operation,                                         # Time a callable with repeats
    summarise,                                              # Convert timing list -> stats dict
    print_summary_line,                                     # Print a single summary block
    write_summary_csv,                                      # Append a CSV row to summary file
    )

load_dotenv()                                               # Load connection details from .env

REPEATS = int(os.getenv("REPEATS", "5"))                                # Number of times to repeat each operation
READ_ID = int(os.getenv("READ_ID", "1"))                                # Lookup ID/key/seq (clamped to dataset size at runtime)
ISOLATED_MODE = os.getenv("ISOLATED_MODE", "false").lower() == "true"   # Run a single operation only
ISOLATED_OP = os.getenv("ISOLATED_OP", "full_scan").lower()             # Operation to benchmark in isolation

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

# Decide which adaptor module to use
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


# Run one benchmark cycle for the selected database
def main():
    db_type = os.getenv("DB_TYPE","sqlite").lower()              # DB to test (sqlite default)
    reset = os.getenv("RESET_DATA", "true").lower() == "true"    # Scenario mode cold/warm flag
    adaptor = cast(Any, select_adaptor(db_type))                 # Load chosen adaptor

    # Build dataset for this run
    dataset_size = max(2, int(os.getenv("DATASET_SIZE", "3")))   # Never allow < 2 records
    records = generate_dataset(dataset_size)
    read_id = max(1, min(READ_ID, len(records)))                 # Clamp READ_ID to the dataset range (avoids lookup on an ID that doesn't exist)
    print(f"\nDB_TYPE: {db_type}")

    # Print a preview of generated values (keeps large runs readable)
    if len(records) > 10:
        preview = records[:5] + ["..."] + records[-5:]
    else:
        preview = records
    print("Generated dataset:", preview)
    os.makedirs("results", exist_ok=True)       # Ensure results folder exists for CSV output

    # -------------------------------------------------------------------------
    # SQLite / MySQL
    # -------------------------------------------------------------------------
    if db_type in ("sqlite", "mysql"):
        conn = adaptor.connect()        # Open database 
        adaptor.create_table(conn)      # Create table if needed

        # Scenario mode only: optionally clear table for a cold run
        if reset:
            adaptor.reset_table(conn)

        # --- ISOLATED MODE (SQLite / MySQL) ---
        if ISOLATED_MODE:
            # Isolated mode always starts from an empty table (treated as cold)
            adaptor.reset_table(conn)

            if ISOLATED_OP in ("full_scan", "primary_key_lookup"):
                adaptor.insert_records(conn, records)

            print(f"\n[ISOLATED MODE] Operation: {ISOLATED_OP.upper()}")

            # Each repeat inserts into an empty table (prevents state carry-over)
            if ISOLATED_OP == "insert":
                def isolated_insert():
                    adaptor.reset_table(conn)
                    adaptor.insert_records(conn, records)
                t = time_operation(isolated_insert, repeats=REPEATS)
            elif ISOLATED_OP == "full_scan":
                t = time_operation(adaptor.read_all, repeats=REPEATS, conn=conn)
            elif ISOLATED_OP == "primary_key_lookup":
                t = time_operation(adaptor.read_by_id, repeats=REPEATS, conn=conn, record_id=read_id)

            # Each repeat resets + re-inserts baseline before updating 
            elif ISOLATED_OP == "update":
                def isolated_update():
                    adaptor.reset_table(conn)
                    adaptor.insert_records(conn, records)
                    adaptor.update_record(conn=conn, record_id=read_id, new_value=f"{records[0]} (updated)")
                t = time_operation(isolated_update, repeats=REPEATS)

            # Each repeat resets + re-inserts baseline before deleting 
            elif ISOLATED_OP == "delete":
                def isolated_delete():
                    adaptor.reset_table(conn)
                    adaptor.insert_records(conn, records)
                    adaptor.delete_record(conn=conn, record_id=read_id)
                t = time_operation(isolated_delete, repeats=REPEATS)
            else:
                raise ValueError(f"Unsupported ISOLATED_OP: {ISOLATED_OP}")

            isolated_run_type = "cold"      # Isolated mode always resets state

            summary_path = (
                f"results/summary_{datetime.datetime.now(datetime.UTC).strftime('%Y%m%d')}_"
                f"{db_type}_{isolated_run_type}_{len(records)}_isolated.csv"
            )

            write_summary_csv(summary_path, db_type, ISOLATED_OP, len(records),
                              isolated_run_type, summarise(t))
            print_summary_line(db_type, ISOLATED_OP, len(records),
                               isolated_run_type, summarise(t))

            conn.close()
            return

        # --- SCENARIO MODE (SQLite / MySQL) ---
        # Insert is repeated REPEATS times, so table size may increase during the run
        def scenario_insert_once():
            if reset:
                adaptor.reset_table(conn)
            adaptor.insert_records(conn, records)

        t = time_operation(scenario_insert_once, repeats=REPEATS)
        print("\nTable Size After INSERT:", display_after(adaptor.read_all(conn)))
        print_summary_line(db_type, "insert", len(records), "cold" if reset else "warm", summarise(t))

        # One summary CSV per database + run type + dataset size + day
        summary_path = (f"results/summary_{datetime.datetime.now(datetime.UTC).strftime('%Y%m%d')}_"
                        f"{db_type}_{'cold' if reset else 'warm'}_{len(records)}.csv"
                    )
        write_summary_csv(summary_path, db_type, "insert", len(records), "cold" if reset else "warm", summarise(t))

        # Full scan (read all rows)
        def scenario_full_scan():
            if reset:
                adaptor.reset_table(conn)
            adaptor.insert_records(conn, records)
            adaptor.read_all(conn)

        t = time_operation(scenario_full_scan, repeats=REPEATS)
        print("\nFull Scan returned:", display_after(adaptor.read_all(conn)))
        print_summary_line(db_type, "full_scan", len(records), "cold" if reset else "warm", summarise(t))
        write_summary_csv(summary_path, db_type, "full_scan", len(records), "cold" if reset else "warm", summarise(t))

        # Primary key lookup (single row)
        def scenario_pk_lookup():
            if reset:
                adaptor.reset_table(conn)
            adaptor.insert_records(conn, records)
            adaptor.read_by_id(conn, read_id)

        t = time_operation(scenario_pk_lookup, repeats=REPEATS)
        result = adaptor.read_by_id(conn, read_id)
        print(f"\nPrimary Key Lookup returned (id={read_id}):", result)
        print_summary_line(db_type, "primary_key_lookup", len(records), "cold" if reset else "warm", summarise(t))
        write_summary_csv(summary_path, db_type, "primary_key_lookup", len(records), "cold" if reset else "warm", summarise(t))

        # Update record 1 (kept fixed for consistency across runs)
        def scenario_update():
            if reset:
                adaptor.reset_table(conn)
            adaptor.insert_records(conn, records)
            adaptor.update_record(conn=conn, record_id=read_id, new_value=f"{records[0]} (updated)")

        t = time_operation(scenario_update, repeats=REPEATS)
        print("\nTable Size After UPDATE:", display_after(adaptor.read_all(conn)))
        print_summary_line(db_type, "update", len(records), "cold" if reset else "warm", summarise(t))
        write_summary_csv(summary_path, db_type, "update", len(records), "cold" if reset else "warm", summarise(t))

        # Delete record 2 (kept fixed for consistency across runs)
        def scenario_delete():
            if reset:
                adaptor.reset_table(conn)
            adaptor.insert_records(conn, records)
            adaptor.delete_record(conn=conn, record_id=read_id)

        t = time_operation(scenario_delete, repeats=REPEATS)
        print("\nTable Size After DELETE:", display_after(adaptor.read_all(conn)))
        print_summary_line(db_type, "delete", len(records), "cold" if reset else "warm", summarise(t))
        write_summary_csv(summary_path, db_type, "delete", len(records), "cold" if reset else "warm", summarise(t))

        conn.close()    # Close database connection


    # -------------------------------------------------------------------------
    # MongoDB
    # -------------------------------------------------------------------------
    elif db_type == "mongodb":
        db: Any = adaptor.connect()           # Connect to MongoDB database

        # Scenario mode only: optionally drop collection for a cold run
        if reset:
            db.records.drop()
            db.meta.delete_one({"_id": "record_seq"})   # Reset counter for cold runs
            db.records.create_index("seq", unique=True)
        
        # --- ISOLATED MODE (MongoDB) ---
        if ISOLATED_MODE:
            db.records.drop()
            db.records.create_index("seq")  

            if ISOLATED_OP in ("full_scan", "primary_key_lookup"):
                adaptor.insert_records(db, records)

            print(f"\n[ISOLATED MODE] Operation: {ISOLATED_OP.upper()}")

            if ISOLATED_OP == "insert":
                # Each repeat inserts into an empty collection (prevents state carry-over)
                def isolated_insert():
                    db.records.drop()
                    db.records.create_index("seq")
                    adaptor.insert_records(db, records)
                t = time_operation(isolated_insert, repeats=REPEATS)
            elif ISOLATED_OP == "full_scan":
                t = time_operation(adaptor.read_all, repeats=REPEATS, db=db)
            elif ISOLATED_OP == "primary_key_lookup":
                t = time_operation(adaptor.read_by_id, repeats=REPEATS, db=db, seq=read_id)

            # Each repeat starts from the same baseline dataset 
            elif ISOLATED_OP == "update":
                def isolated_update():
                    db.records.drop()
                    db.records.create_index("seq")
                    adaptor.insert_records(db, records)
                    adaptor.update_record(db=db, seq=read_id, new_name=f"{records[0]} (updated)")
                t = time_operation(isolated_update, repeats=REPEATS)

            # Each repeat starts from the same baseline dataset 
            elif ISOLATED_OP == "delete":
    
                def isolated_delete():
                    db.records.drop()
                    db.records.create_index("seq")
                    adaptor.insert_records(db, records)
                    adaptor.delete_record(db=db, seq=read_id)
                t = time_operation(isolated_delete, repeats=REPEATS)
            else:
                raise ValueError(f"Unsupported ISOLATED_OP: {ISOLATED_OP}")

            # In isolated mode, the run is always effectively "cold"
            isolated_run_type = "cold"

            summary_path = (
                f"results/summary_{datetime.datetime.now(datetime.UTC).strftime('%Y%m%d')}_"
                f"{db_type}_{isolated_run_type}_{len(records)}_isolated.csv"
            )

            write_summary_csv(summary_path, db_type, ISOLATED_OP, len(records),
                              isolated_run_type, summarise(t))
            print_summary_line(db_type, ISOLATED_OP, len(records),
                               isolated_run_type, summarise(t))
            return


        # --- SCENARIO MODE (MongoDB) ---
        # Note: insert is repeated REPEATS times, so collection size may increase during the run
        t = time_operation(adaptor.insert_records, repeats=REPEATS, db=db, records=records)
        print("\nTable Size After INSERT:", display_after(adaptor.read_all(db)))
        print_summary_line(db_type, "insert", len(records), "cold" if reset else "warm", summarise(t))

        summary_path = (f"results/summary_{datetime.datetime.now(datetime.UTC).strftime('%Y%m%d')}_"
                        f"{db_type}_{'cold' if reset else 'warm'}_{len(records)}.csv"
                    )
        write_summary_csv(summary_path, db_type, "insert", len(records), "cold" if reset else "warm", summarise(t))

        t = time_operation(adaptor.read_all, repeats=REPEATS, db=db)
        print("\nFull Scan returned:", display_after(adaptor.read_all(db)))
        print_summary_line(db_type, "full_scan", len(records), "cold" if reset else "warm", summarise(t))
        write_summary_csv(summary_path, db_type, "full_scan", len(records), "cold" if reset else "warm", summarise(t))

        t = time_operation(adaptor.read_by_id, repeats=REPEATS, db=db, seq=read_id)
        result = adaptor.read_by_id(db, read_id)
        print(f"\nPrimary Key Lookup returned (id={read_id}):", result)
        print_summary_line(db_type, "primary_key_lookup", len(records), "cold" if reset else "warm", summarise(t))
        write_summary_csv(summary_path, db_type, "primary_key_lookup", len(records), "cold" if reset else "warm", summarise(t))

        t = time_operation(adaptor.update_record, repeats=REPEATS, db=db, seq=1, new_name=f"{records[0]} (updated)")
        print("\nTable Size After UPDATE:", display_after(adaptor.read_all(db)))
        print_summary_line(db_type, "update", len(records), "cold" if reset else "warm", summarise(t))
        write_summary_csv(summary_path, db_type, "update", len(records), "cold" if reset else "warm", summarise(t))

        t = time_operation(adaptor.delete_record, repeats=REPEATS, db=db, seq=2)
        print("\nTable Size After DELETE:", display_after(adaptor.read_all(db)))
        print_summary_line(db_type, "delete", len(records), "cold" if reset else "warm", summarise(t))
        write_summary_csv(summary_path, db_type, "delete", len(records), "cold" if reset else "warm", summarise(t))


    # -------------------------------------------------------------------------
    # Redis
    # -------------------------------------------------------------------------
    elif db_type == "redis":
        r = adaptor.connect()           # Connect to Redis server

        # Scenario mode only: flush store for a cold run
        if reset:
            adaptor.reset_store(r)
        
        # --- ISOLATED MODE (Redis) ---
        if ISOLATED_MODE:
            # Isolated mode always starts clean (treated as cold)
            adaptor.reset_store(r)

            # For scans/lookups, insert baseline data once before timing
            if ISOLATED_OP in ("full_scan", "primary_key_lookup"):
                adaptor.insert_records(r, records)

            print(f"\n[ISOLATED MODE] Operation: {ISOLATED_OP.upper()}")

            if ISOLATED_OP == "insert":
                # Each repeat inserts into an empty store (prevents state carry-over)
                def isolated_insert():
                    adaptor.reset_store(r)
                    adaptor.insert_records(r, records)

                t = time_operation(isolated_insert, repeats=REPEATS)

            elif ISOLATED_OP == "full_scan":
                t = time_operation(adaptor.read_all, repeats=REPEATS, r=r)

            elif ISOLATED_OP == "primary_key_lookup":
                t = time_operation(adaptor.read_by_id, repeats=REPEATS, r=r, record_id=read_id)

            # Each repeat resets + re-inserts baseline before updating 
            elif ISOLATED_OP == "update":
                def isolated_update():
                    adaptor.reset_store(r)
                    adaptor.insert_records(r, records)
                    adaptor.update_record(r=r, record_id=read_id, new_value=f"{records[0]} (updated)")
                t = time_operation(isolated_update, repeats=REPEATS)

            # Each repeat resets + re-inserts baseline before deleting 
            elif ISOLATED_OP == "delete":
                def isolated_delete():
                    adaptor.reset_store(r)
                    adaptor.insert_records(r, records)
                    adaptor.delete_record(r=r, record_id=read_id)
                t = time_operation(isolated_delete, repeats=REPEATS)
            else:
                raise ValueError(f"Unsupported ISOLATED_OP: {ISOLATED_OP}")

            # In isolated mode, the run is always effectively "cold"
            isolated_run_type = "cold"

            summary_path = (
                f"results/summary_{datetime.datetime.now(datetime.UTC).strftime('%Y%m%d')}_"
                f"{db_type}_{isolated_run_type}_{len(records)}_isolated.csv"
            )

            write_summary_csv(summary_path, db_type, ISOLATED_OP, len(records),
                              isolated_run_type, summarise(t))
            print_summary_line(db_type, ISOLATED_OP, len(records),
                               isolated_run_type, summarise(t))
            return


        # --- SCENARIO MODE (Redis) ---
        # Insert is repeated REPEATS times, so keys may accumulate during the run
        t = time_operation(adaptor.insert_records, repeats=REPEATS, r=r, records=records)
        print("\nTable Size After INSERT:", display_after(adaptor.read_all(r)))
        print_summary_line(db_type, "insert", len(records), "cold" if reset else "warm", summarise(t))

        summary_path = (f"results/summary_{datetime.datetime.now(datetime.UTC).strftime('%Y%m%d')}_"
                        f"{db_type}_{'cold' if reset else 'warm'}_{len(records)}.csv"
                    )
        write_summary_csv(summary_path, db_type, "insert", len(records), "cold" if reset else "warm", summarise(t))

        t = time_operation(adaptor.read_all, repeats=REPEATS, r=r)
        print("\nFull Scan returned:", display_after(adaptor.read_all(r)))
        print_summary_line(db_type, "full_scan", len(records), "cold" if reset else "warm", summarise(t))
        write_summary_csv(summary_path, db_type, "full_scan", len(records), "cold" if reset else "warm", summarise(t))

        t = time_operation(adaptor.read_by_id, repeats=REPEATS, r=r, record_id=read_id)
        result = adaptor.read_by_id(r, read_id)
        print(f"\nPrimary Key Lookup returned (id={read_id}):", result)
        print_summary_line(db_type, "primary_key_lookup", len(records), "cold" if reset else "warm", summarise(t))
        write_summary_csv(summary_path, db_type, "primary_key_lookup", len(records), "cold" if reset else "warm", summarise(t))

        t = time_operation(adaptor.update_record, repeats=REPEATS, r=r, record_id=1, new_value=f"{records[0]} (updated)")
        print("\nTable Size After UPDATE:", display_after(adaptor.read_all(r)))
        print_summary_line(db_type, "update", len(records), "cold" if reset else "warm", summarise(t))
        write_summary_csv(summary_path, db_type, "update", len(records), "cold" if reset else "warm", summarise(t))

        t = time_operation(adaptor.delete_record, repeats=REPEATS, r=r, record_id=2)
        print("\nTable Size After DELETE:", display_after(adaptor.read_all(r)))
        print_summary_line(db_type, "delete", len(records), "cold" if reset else "warm", summarise(t))
        write_summary_csv(summary_path, db_type, "delete", len(records), "cold" if reset else "warm", summarise(t))


# Only run main() if this file is run directly
if __name__ == "__main__":
    main()
    print()  # Adds a blank line before returning to terminal
