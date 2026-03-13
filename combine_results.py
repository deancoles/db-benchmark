"""
combine_results.py
------------------
What this file does:
    - Reads all benchmark CSV files from the results folder
    - Combines them into a single master dataset
    - Sorts the results for readability
    - Saves the merged file as master_benchmark_results.csv

Used by:
    - Dissertation analysis and graph generation
"""

import pandas as pd         # Load and combine CSV datasets
import glob                 # Locate multiple CSV files using patterns
import os                   # Build file paths in a cross-platform way

# Defines the folder containing benchmark results and grabs all CSV files
results_folder = "results"      
files = glob.glob(os.path.join(results_folder, "*.csv"))

dataframes = []             # Store each CSV file as a dataframe

for file in files:
    df = pd.read_csv(file)
    dataframes.append(df)

# Combine all dataframes into one master dataframe and sort for readability
combined = pd.concat(dataframes, ignore_index=True)
combined = combined.sort_values(by=["db", "records", "operation"]).reset_index(drop=True)   

# Save the master results file
combined.to_csv("master_benchmark_results.csv", index=False)

# Print a quick confirmation showing a preview of the dataset
print("Combined files:", len(files))
print("Total rows:", len(combined))
print("Saved as: master_benchmark_results.csv")
print()
print(combined.head(20))
