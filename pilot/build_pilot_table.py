"""
build_pilot_table.py
--------------------
What this file does:
    - Combines pilot benchmark result files
    - Builds summary tables for each dataset size
    - Saves pilot tables as separate CSV files

Used by:
    Pilot benchmarking analysis only
"""

import pandas as pd                 # Data analysis and table creation
import glob                         # Locate pilot result CSV files

# Locate pilot results and merge results into one dataframe
files = glob.glob("pilot_results/*.csv")       
dfs = [pd.read_csv(file) for file in files]
combined = pd.concat(dfs, ignore_index=True)

combined = combined[["db", "records", "run_type", "operation", "mean_time"]]

for record_count in sorted(combined["records"].unique()):
    subset = combined[combined["records"] == record_count]

    table = subset.pivot_table(
        index=["db", "run_type"],
        columns="operation",
        values="mean_time"
    ).reset_index()

    output_name = f"pilot_table_{record_count}.csv"
    table.to_csv(output_name, index=False)
    print(f"\nPilot table for {record_count} records:\n")
    print(table)
    print(f"\nSaved to {output_name}")