"""
benchmark_utils.py
------------------
What this file does:
    - Times how long operations take
    - Runs each operation several times
    - Works out simple stats (fastest, slowest, average, middle value)
    - Prints a one-line summary you can read quickly

Used by:
    runner.py (to measure database inserts, reads, updates, deletes)
"""

from __future__ import annotations                        # Enable future-style annotations 
import statistics                                         # For averages and medians
import time                                               # For timing and sleep in test
from typing import Callable, Iterable, Any, Dict, List    # Simple type hints for clarity


# Time a function once and return how long it took
def _time_once(fn: Callable, *args, **kwargs) -> float:
    start = time.perf_counter()           # Start timer
    fn(*args, **kwargs)                   # Run function
    end = time.perf_counter()             # End timer
    return end - start                    # Elapsed time in seconds

# Time a function several times and collect the results
def time_operation(fn: Callable, repeats: int = 5, *args, **kwargs) -> List[float]:
    durations: List[float] = []           # List to store each run’s time

    # Repeat chosen number of times and add each timing to durations
    for _ in range(max(1, repeats)):    
        durations.append(_time_once(fn, *args, **kwargs))    
    return durations                      

# Work out simple stats from a list of times
def summarise(durations: Iterable[float]) -> Dict[str, float]:
    xs = list(durations)                  # Convert to list
    xs.sort()                             # Sort times (needed for percentiles)
    n = len(xs)                           # How many timings received

    # If no data, just return zeros
    if n == 0:
        return {
            "n":0, "min": 0.0, "max": 0.0, "mean": 0.0,
            "median": 0.0, "p25":0.0, "p75": 0.0, "iqr": 0.0
        }
    p25 = _percentile(xs, 25)             # 25% point
    p75 = _percentile(xs, 75)             # 75% point
    return{
        "n": float(n),                    # Number of runs
        "min": xs[0],                     # Fastest
        "max": xs[-1],                    # Slowest
        "mean": statistics.fmean(xs),     # Average
        "median": statistics.median(xs),  # Middle value
        "p25": p25,                       # Lower range
        "p75": p75,                       # Upper range
        "iqr": p75 - p25,                 # Gap between upper and lower
    }

# Find a chosen percentile (like 25% or 75%)
def _percentile(xs: List[float], p: float) -> float:

    # If list is empty
    if not xs:
        return 0.0
    
    # Lowest value
    if p <= 0:
        return xs[0]
    
    # Highest value
    if p >= 100:
        return xs[-1]
    
    k = (len(xs) - 1) * (p / 100.0)      # Position in the list
    f = int(k)                           # Lower index
    c = min(f + 1, len(xs) - 1)          # Upper index

    # If exact index, return it
    if f == c:
        return xs[f]
    
    # Mix values if between two positions
    d0 = xs[f] * (c - k)                 # Lower part         
    d1 = xs[c] * (k - f)                 # Upper part        
    return d0 + d1                       # Combine

# Print a short line with the stats
def print_summary_line(
        db_type: str,
        op_name: str,
        dataset_size: int,
        run_type: str,
        stats: Dict[str, float]
) -> None:
    
    line = (
        f"{db_type} {op_name.upper()} n={int(stats['n'])} size={dataset_size} {run_type} "
        f"mean={stats['mean']:.6f}s  p50={stats['median']:.6f}s  "
        f"IQR={stats['iqr']:.6f}s  min={stats['min']:.6f}s  max={stats['max']:.6f}s"
    )
    print(line)


# Run a test if this file is executed directly
if __name__ == "__main__":
    def faux_op():
        time.sleep(0.001)                        # Wait 1ms to act like a database action

    times = time_operation(faux_op, repeats=5)   # Collect timings
    stats = summarise(times)                     # Summarise results
    print_summary_line("demo", "sleep", dataset_size=3, run_type="cold", stats=stats)
