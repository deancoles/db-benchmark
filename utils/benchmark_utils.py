"""
benchmark_utils.py
------------------
What this file does:
    - Times how long operations take
    - Repeats each operation and collects timings
    - Works out simple stats (fastest, slowest, average, middle value)
    - Prints a clear multi-line summary (3 decimal places on screen)
    - Saves one summary row per operation to CSV (6 decimal places)

Used by:
    runner.py (to measure database inserts, reads, updates, deletes)
"""

from __future__ import annotations                        # Enable future-style annotations 
import statistics                                         # For averages and medians
import csv, os, datetime, time
from typing import Callable, Iterable, Any, Dict, List    # Simple type hints for clarity


# Time a function once and return the elapsed seconds (float).
def _time_once(fn: Callable, *args, **kwargs) -> float:
    start = time.perf_counter()           # Start timer
    fn(*args, **kwargs)                   # Run function
    end = time.perf_counter()             # End timer
    return end - start                    # Elapsed time in seconds

# Run a function several times and collect durations
def time_operation(fn: Callable, repeats: int = 5, *args, **kwargs) -> List[float]:
    durations: List[float] = []           # List to store each run’s time

    # Always run at least once even if repeats <= 0 (safety guard).
    for _ in range(max(1, repeats)):    
        durations.append(_time_once(fn, *args, **kwargs))    
    return durations                      

# Turn durations into simple stats (seconds)
def summarise(durations: Iterable[float]) -> Dict[str, float]:
    xs = list(durations)                  # Convert to list
    n = len(xs)                           # How many timings received

    # If no data, just return zeros
    if n == 0:
        return {
            "n": 0,
            "min": 0.0,
            "max": 0.0,
            "mean": 0.0,
            "median": 0.0
        }

    return {
        "n": float(n),                    # Number of runs
        "min": xs[0],                     # Fastest
        "max": xs[-1],                    # Slowest
        "mean": statistics.fmean(xs),     # Average
        "median": statistics.median(xs)   # Middle value
    }


# Print a neat multi-line block (3 d.p. on screen)
def print_summary_line(
        db_type: str,
        op_name: str,
        dataset_size: int,
        run_type: str,
        stats: Dict[str, float]
) -> None:
    
    print(
        f"{db_type} {op_name.upper()} ({run_type})\n"
        f"  runs={int(stats['n'])}  records={dataset_size}\n"
        f"  mean={stats['mean']:.3f}s  median={stats['median']:.3f}s\n"
        f"  min={stats['min']:.3f}s  max={stats['max']:.3f}s"
    )

# Append results to CSV; write a header if the file is new
def write_summary_csv(
    path: str,
    db_type: str,
    op_name: str,
    dataset_size: int,
    run_type: str,
    stats: Dict[str, float]
) -> None:
  
    os.makedirs(os.path.dirname(path), exist_ok=True)
    write_header = not os.path.exists(path)

    row = [
        datetime.datetime.now(datetime.UTC).isoformat(),
        db_type,
        op_name,
        dataset_size,
        run_type,
        int(stats["n"]),
        f"{stats['mean']:.6f}",
        f"{stats['median']:.6f}",
        f"{stats['min']:.6f}",
        f"{stats['max']:.6f}",
    ]

    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            # times are seconds
            writer.writerow(
                ["timestamp", "db", "operation", "records", "run_type",
                 "runs", "mean_time", "median_time", "min_time", "max_time"]
            )
        writer.writerow(row)


# Run a test if this file is executed directly
if __name__ == "__main__":
    # Allow dataset size to come from .env (default 3)
    from dotenv import load_dotenv
    load_dotenv()
    import os
    DATASET_SIZE = int(os.getenv("DATASET_SIZE", "3"))

    # Sleeps for 1ms to simulate a database action
    def faux_op():
        time.sleep(0.001)  

    durations = time_operation(faux_op, repeats=5)   # Collect timings
    stats = summarise(durations)                     # Summarise results
    print_summary_line("demo", "sleep", dataset_size=DATASET_SIZE, run_type="cold", stats=stats)
    print("\n(Demo complete: above output shows multi-line format with 3 decimal places.)")
