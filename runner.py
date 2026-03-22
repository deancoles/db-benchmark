"""
runner.py
---------
Runs benchmarking for the selected database adaptor.

What this script does:
  - Generates a dataset of placeholder record values (size from .env)
  - Scenario mode: runs INSERT → FULL_SCAN → LOOKUP → UPDATE → DELETE
  - Isolated mode: runs one chosen operation on a reset baseline dataset
  - Prints timing summaries and writes results to CSV files in the results folder

Controlled by .env:
  DB_TYPE        sqlite | mysql | mongodb | redis
  DATASET_SIZE   number of records generated per benchmark run
  REPEATS        number of repeats per timed operation
  ISOLATED_MODE  runs one selected operation independently, resetting the database state before each 
                 repeat so that the operation is measured without influence from other benchmark steps
  ISOLATED_OP    insert | full_scan | lookup | update | delete
  READ_ID        record/key/seq used for lookup operations
  OUTPUT_DETAIL  count | preview | full (how much data is printed after each operation)
"""

import os                                                               # Access environment variables
import datetime                                                         # Timestamped CSV filenames
from datasets.dataset_generator import generate_dataset                 # Make test dataset
from dotenv import load_dotenv                                          # Load values from .env
from typing import Any, cast                                            # Typing support
from utils.benchmark_utils import (
    time_operation,                                                     # Time a callable with repeats
    summarise,                                                          # Convert timing list -> stats dict
    print_summary_line,                                                 # Print a single summary block
    write_summary_csv,                                                  # Append a CSV row to summary file
    )

load_dotenv()                                                           # Load connection details from .env

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
def display_after(items=None, count=None):
    if OUTPUT_DETAIL == "count":
        if count is not None:
            return f"{count} items"
        items = list(items) if items is not None else []
        return f"{len(items)} items"
    elif OUTPUT_DETAIL == "preview":
        items = list(items) if items is not None else []
        return preview_list(items)
    else:
        items = list(items) if items is not None else []
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


# Record and display benchmark results
def record_results(summary_path, db_type, operation, records, run_type, timings):
    stats = summarise(timings)
    print_summary_line(db_type, operation, records, run_type, stats)
    write_summary_csv(summary_path, db_type, operation, records, run_type, stats)


# Build the CSV output path for a benchmark run
def build_summary_path(db_type, run_type, record_count, isolated=False):
    suffix = "_isolated" if isolated else ""
    return (
        f"results/summary_{datetime.datetime.now(datetime.UTC).strftime('%Y%m%d')}_"
        f"{db_type}_{run_type}_{record_count}{suffix}.csv"
    )


# Run one benchmark cycle for the selected database
def main():
    db_type = os.getenv("DB_TYPE","sqlite").lower()              # DB to test (sqlite default if none given)

    valid_db_types = {"sqlite", "mysql", "mongodb", "redis"}
    if db_type not in valid_db_types:
        raise ValueError(
            f"Invalid DB_TYPE '{db_type}'. Valid options are: sqlite, mysql, mongodb, redis"
        )

    adaptor = cast(Any, select_adaptor(db_type))                 # Load chosen adaptor

    # Build dataset for this run
    dataset_size = max(2, int(os.getenv("DATASET_SIZE", "3")))   # Never allow < 2 records
    records = generate_dataset(dataset_size)
    read_id = max(1, min(READ_ID, len(records)))                 # Clamp to the dataset range (avoids lookup on an ID that doesn't exist)
    print(f"\nDB_TYPE: {db_type}")

    # Print a preview of generated values (keeps large runs readable)
    if len(records) > 10:
        preview = records[:5] + ["..."] + records[-5:]
    else:
        preview = records
    print("Generated dataset:", preview)
    os.makedirs("results", exist_ok=True)                        # Ensure results folder exists for CSV output


    # -------------------------------------------------------------------------
    # SQLite / MySQL
    # -------------------------------------------------------------------------

    if db_type in ("sqlite", "mysql"):
        conn = adaptor.connect()        
        adaptor.create_table(conn)      

        # --- ISOLATED MODE (SQLite / MySQL) ---
        # Runs a single selected operation independently from the scenario workflow.
        # The database state is reset before each repeat to ensure the operation
        # is measured without interference from previous benchmark steps.
        if ISOLATED_MODE:
            adaptor.reset_table(conn)

            if ISOLATED_OP in ("full_scan", "lookup"):
                adaptor.insert_records(conn, records)

            print(f"\n[ISOLATED MODE] Operation: {ISOLATED_OP.upper()}")

            if ISOLATED_OP == "insert":
                def isolated_insert():
                    adaptor.reset_table(conn)
                    adaptor.insert_records(conn, records)
                t = time_operation(isolated_insert, repeats=REPEATS)
            elif ISOLATED_OP == "full_scan":
                t = time_operation(adaptor.read_all, repeats=REPEATS, conn=conn)
            elif ISOLATED_OP == "lookup":
                t = time_operation(adaptor.read_by_id, repeats=REPEATS, conn=conn, record_id=read_id)
            elif ISOLATED_OP == "update":
                def isolated_update():
                    adaptor.reset_table(conn)
                    adaptor.insert_records(conn, records)
                    adaptor.update_record(conn=conn, record_id=read_id, new_value=f"{records[0]} (updated)")
                t = time_operation(isolated_update, repeats=REPEATS)
            elif ISOLATED_OP == "delete":
                def isolated_delete():
                    adaptor.reset_table(conn)
                    adaptor.insert_records(conn, records)
                    adaptor.delete_record(conn=conn, record_id=read_id)
                t = time_operation(isolated_delete, repeats=REPEATS)
            else:
                raise ValueError(f"Unsupported ISOLATED_OP: {ISOLATED_OP}")

            isolated_run_type = "cold"      # Isolated benchmarks are always treated as cold runs
            summary_path = build_summary_path(db_type, isolated_run_type, len(records), isolated=True)
            record_results(summary_path, db_type, ISOLATED_OP, len(records), isolated_run_type, t)
            conn.close()
            return


        # --- SCENARIO MODE (SQLite / MySQL) ---
        run_type = "cold"
        summary_path = build_summary_path(db_type, run_type, len(records))

        # Reset the database state and insert the benchmark dataset
        def reset_and_seed_sql():
            adaptor.reset_table(conn)
            adaptor.insert_records(conn, records)
        
        # Insert benchmark
        def scenario_insert_sql():
            reset_and_seed_sql()

        # Measures time required to insert the dataset
        t = time_operation(scenario_insert_sql, repeats=REPEATS)
        print("\nTable Size After INSERT:", display_after(count=adaptor.count_records(conn)))
        record_results(summary_path, db_type, "insert", len(records), run_type, t)

        # Full scan benchmark
        def scenario_full_scan_sql():
            reset_and_seed_sql()
            adaptor.read_all(conn)

        # Measures time required to perform a full scan
        t = time_operation(scenario_full_scan_sql, repeats=REPEATS)
        print("\nFull Scan returned:", display_after(adaptor.read_all(conn)))
        record_results(summary_path, db_type, "full_scan", len(records), run_type, t)

        # Lookup benchmark
        def scenario_lookup_sql():
            reset_and_seed_sql()
            adaptor.read_by_id(conn, read_id)

        # Measures time required to retrieve one record by id
        t = time_operation(scenario_lookup_sql, repeats=REPEATS)
        reset_and_seed_sql()
        result = adaptor.read_by_id(conn, read_id)
        print(f"\nLookup returned (id={read_id}):", result)
        record_results(summary_path, db_type, "lookup", len(records), run_type, t)

        # Update benchmark
        def scenario_update_sql():
            reset_and_seed_sql()
            adaptor.update_record(conn=conn, record_id=read_id, new_value=f"{records[0]} (updated)")

        # Measures time required to update one record
        t = time_operation(scenario_update_sql, repeats=REPEATS)
        reset_and_seed_sql()
        adaptor.update_record(conn=conn, record_id=read_id, new_value=f"{records[0]} (updated)")
        print("\nTable Size After UPDATE:", display_after(count=adaptor.count_records(conn)))
        record_results(summary_path, db_type, "update", len(records), run_type, t)

        # Delete benchmark
        def scenario_delete_sql():
            reset_and_seed_sql()
            adaptor.delete_record(conn=conn, record_id=read_id)

        # Measures time required to delete one record
        t = time_operation(scenario_delete_sql, repeats=REPEATS)
        reset_and_seed_sql()
        adaptor.delete_record(conn=conn, record_id=read_id)
        print("\nTable Size After DELETE:", display_after(count=adaptor.count_records(conn)))
        record_results(summary_path, db_type, "delete", len(records), run_type, t)

        conn.close()    


    # -------------------------------------------------------------------------
    # MongoDB
    # -------------------------------------------------------------------------
    
    elif db_type == "mongodb":
        db: Any = adaptor.connect()                    
        
        # --- ISOLATED MODE (MongoDB) ---
        if ISOLATED_MODE:
            db.records.drop()
            db.meta.delete_one({"_id": "record_seq"})
            db.records.create_index("seq", unique=True)  

            if ISOLATED_OP in ("full_scan", "lookup"):
                adaptor.insert_records(db, records)

            print(f"\n[ISOLATED MODE] Operation: {ISOLATED_OP.upper()}")

            if ISOLATED_OP == "insert":
                def isolated_insert():
                    db.records.drop()
                    db.meta.delete_one({"_id": "record_seq"})
                    db.records.create_index("seq", unique=True)  
                    adaptor.insert_records(db, records)
                t = time_operation(isolated_insert, repeats=REPEATS)
            elif ISOLATED_OP == "full_scan":
                t = time_operation(adaptor.read_all, repeats=REPEATS, db=db)
            elif ISOLATED_OP == "lookup":
                t = time_operation(adaptor.read_by_id, repeats=REPEATS, db=db, seq=read_id)
            elif ISOLATED_OP == "update":
                def isolated_update():
                    db.records.drop()
                    db.meta.delete_one({"_id": "record_seq"})
                    db.records.create_index("seq", unique=True)  
                    adaptor.insert_records(db, records)
                    adaptor.update_record(db=db, seq=read_id, new_name=f"{records[0]} (updated)")
                t = time_operation(isolated_update, repeats=REPEATS)
            elif ISOLATED_OP == "delete":
                def isolated_delete():
                    db.records.drop()
                    db.meta.delete_one({"_id": "record_seq"})
                    db.records.create_index("seq", unique=True)  
                    adaptor.insert_records(db, records)
                    adaptor.delete_record(db=db, seq=read_id)
                t = time_operation(isolated_delete, repeats=REPEATS)
            else:
                raise ValueError(f"Unsupported ISOLATED_OP: {ISOLATED_OP}")

            isolated_run_type = "cold"
            summary_path = build_summary_path(db_type, isolated_run_type, len(records), isolated=True)
            record_results(summary_path, db_type, ISOLATED_OP, len(records), isolated_run_type, t)
            return


        # --- SCENARIO MODE (MongoDB) ---
        run_type = "cold" 
        summary_path = build_summary_path(db_type, run_type, len(records))

        # Reset the database state and insert the benchmark dataset
        def reset_and_seed_mongo():
            db.records.drop()
            db.meta.delete_one({"_id": "record_seq"})
            db.records.create_index("seq", unique=True)
            adaptor.insert_records(db,records)

        # Insert benchmark
        def scenario_insert_mongo():
            reset_and_seed_mongo()
        
        # Measures time required to insert the dataset
        t = time_operation(scenario_insert_mongo, repeats=REPEATS)
        print("\nTable Size After INSERT:", display_after(count=adaptor.count_records(db)))
        record_results(summary_path, db_type, "insert", len(records), run_type, t)

        # Full Scan benchmark
        def scenario_full_scan_mongo():
            reset_and_seed_mongo()
            adaptor.read_all(db)

        # Measures time required to perform a full scan
        t = time_operation(scenario_full_scan_mongo, repeats=REPEATS)
        print("\nFull Scan returned:", display_after(adaptor.read_all(db)))
        record_results(summary_path, db_type, "full_scan", len(records), run_type, t)

        # Lookup benchmark
        def scenario_lookup_mongo():
            reset_and_seed_mongo()
            adaptor.read_by_id(db,read_id)

        # Measures time required to retrieve one record by id
        t = time_operation(scenario_lookup_mongo, repeats=REPEATS)
        reset_and_seed_mongo()
        result = adaptor.read_by_id(db, read_id)
        print(f"\nLookup returned (id={read_id}):", result)
        record_results(summary_path, db_type, "lookup", len(records), run_type, t)

        # Update benchmark
        def scenario_update_mongo():
            reset_and_seed_mongo()
            adaptor.update_record(db=db, seq=read_id, new_name=f"{records[0]} (updated)")

        # Measures time required to update one record
        t = time_operation(scenario_update_mongo, repeats=REPEATS)
        reset_and_seed_mongo()
        adaptor.update_record(db=db, seq=read_id, new_name=f"{records[0]} (updated)")
        print("\nTable Size After UPDATE:", display_after(count=adaptor.count_records(db)))
        record_results(summary_path, db_type, "update", len(records), run_type, t)

        # Delete benchmark
        def scenario_delete_mongo():
            reset_and_seed_mongo()
            adaptor.delete_record(db = db,seq=read_id)

        # Measures time taken to delete one record
        t = time_operation(scenario_delete_mongo, repeats=REPEATS)
        reset_and_seed_mongo()
        adaptor.delete_record(db = db,seq=read_id)
        print("\nTable Size After DELETE:", display_after(count=adaptor.count_records(db)))
        record_results(summary_path, db_type, "delete", len(records), run_type, t)


    # -------------------------------------------------------------------------
    # Redis
    # -------------------------------------------------------------------------

    elif db_type == "redis":
        r = adaptor.connect()
        
        # --- ISOLATED MODE (Redis) ---
        if ISOLATED_MODE:
            adaptor.reset_store(r)

            # For scans/lookups, insert baseline data once before timing
            if ISOLATED_OP in ("full_scan", "lookup"):
                adaptor.insert_records(r, records)

            print(f"\n[ISOLATED MODE] Operation: {ISOLATED_OP.upper()}")

            if ISOLATED_OP == "insert":
                def isolated_insert():
                    adaptor.reset_store(r)
                    adaptor.insert_records(r, records)
                t = time_operation(isolated_insert, repeats=REPEATS)
            elif ISOLATED_OP == "full_scan":
                t = time_operation(adaptor.read_all, repeats=REPEATS, r=r)
            elif ISOLATED_OP == "lookup":
                t = time_operation(adaptor.read_by_id, repeats=REPEATS, r=r, record_id=read_id)
            elif ISOLATED_OP == "update":
                def isolated_update():
                    adaptor.reset_store(r)
                    adaptor.insert_records(r, records)
                    adaptor.update_record(r=r, record_id=read_id, new_value=f"{records[0]} (updated)")
                t = time_operation(isolated_update, repeats=REPEATS)
            elif ISOLATED_OP == "delete":
                def isolated_delete():
                    adaptor.reset_store(r)
                    adaptor.insert_records(r, records)
                    adaptor.delete_record(r=r, record_id=read_id)
                t = time_operation(isolated_delete, repeats=REPEATS)
            else:
                raise ValueError(f"Unsupported ISOLATED_OP: {ISOLATED_OP}")

            isolated_run_type = "cold"
            summary_path = build_summary_path(db_type, isolated_run_type, len(records), isolated=True)
            record_results(summary_path, db_type, ISOLATED_OP, len(records), isolated_run_type, t)
            return


        # --- SCENARIO MODE (Redis) ---
        run_type = "cold" 
        summary_path = build_summary_path(db_type, run_type, len(records))

        # Reset the database state and insert the benchmark dataset
        def reset_and_seed_redis():
            adaptor.reset_store(r)
            adaptor.insert_records(r, records)
        
        # Insert benchmark
        def scenario_insert_redis():
            reset_and_seed_redis()

        # Measures time required to insert the dataset
        t = time_operation(scenario_insert_redis, repeats=REPEATS)
        print("\nTable Size After INSERT:", display_after(count=adaptor.count_records(r)))
        record_results(summary_path, db_type, "insert", len(records), run_type, t)

        # Full Scan benchmark
        def scenario_full_scan_redis():
            reset_and_seed_redis()
            adaptor.read_all(r)

        # Measures time required to perform a full scan
        t = time_operation(scenario_full_scan_redis, repeats=REPEATS)
        print("\nFull Scan returned:", display_after(adaptor.read_all(r)))
        record_results(summary_path, db_type, "full_scan", len(records), run_type, t)

        # Lookup benchmark
        def scenario_lookup_redis():
            reset_and_seed_redis()
            adaptor.read_by_id(r, read_id)

        # Measures time required to retrieve one record by id
        t = time_operation(scenario_lookup_redis, repeats=REPEATS)
        reset_and_seed_redis()
        result = adaptor.read_by_id(r, read_id)
        print(f"\nLookup returned (id={read_id}):", result)
        record_results(summary_path, db_type, "lookup", len(records), run_type, t)

        # Update benchmark
        def scenario_update_redis():
            reset_and_seed_redis()
            adaptor.update_record(r=r, record_id=read_id, new_value=f"{records[0]} (updated)")

        # Measures time required to update one record
        t = time_operation(scenario_update_redis, repeats=REPEATS)
        reset_and_seed_redis()
        adaptor.update_record(r=r, record_id=read_id, new_value=f"{records[0]} (updated)")
        print("\nTable Size After UPDATE:", display_after(count=adaptor.count_records(r)))
        record_results(summary_path, db_type, "update", len(records), run_type, t)

        # Delete benchmark
        def scenario_delete_redis():
            reset_and_seed_redis()
            adaptor.delete_record(r=r, record_id=read_id)

        # Measures time required to delete one record
        t = time_operation(scenario_delete_redis, repeats=REPEATS)
        reset_and_seed_redis()
        adaptor.delete_record(r=r, record_id=read_id)
        print("\nTable Size After DELETE:", display_after(count=adaptor.count_records(r)))
        record_results(summary_path, db_type, "delete", len(records), run_type, t)


# Only run main() if this file is run directly
if __name__ == "__main__":
    main()
    print()  
