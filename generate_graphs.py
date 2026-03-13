"""
generate_graphs.py
------------------
What this file does:
    - Reads the combined benchmark results file.
    - Generates one graph per benchmark operation.
    - Saves graphs to the graphs folder.

Used by:
    - Dissertation analysis and results presentation.
"""

import os                                           # File and directory handling
import pandas as pd                                 # Data analysis and CSV loading
import matplotlib.pyplot as plt                     # Graph plotting
import matplotlib.ticker as ticker                  # Custom axis formatting

df = pd.read_csv("master_benchmark_results.csv")    # Load benchmark dataset
os.makedirs("graphs", exist_ok=True)                # Ensure directory exists
plt.style.use("default")                            # Style used in graphs

# Benchmark operations that will each receive a graph
operations = [
    "insert",
    "full_scan",
    "primary_key_lookup",
    "update",
    "delete"
]

databases = df["db"].unique()                       # Identify unique database systems

# Generate a graph for each operation type
for op in operations:

    plt.figure(figsize=(12,5))                      # Create new figure for the current operation
    subset = df[df["operation"] == op]              # Filter dataset to only include current operation

    # Plot a line for each database system
    for db in databases:

        # Extract data for current database and order by record count (prevents lines appearing out of order on graph)
        db_data = subset[subset["db"] == db]
        db_data = db_data.sort_values("records")

        # Plot execution time against dataset size
        plt.plot(
            db_data["records"],
            db_data["mean_time"],
            marker="o",
            linewidth=2,
            label=db
        )

    # Axis labels and graph title
    plt.xlabel("Dataset Size (records)")
    plt.ylabel("Execution Time (s)")
    plt.title(f"{op.replace('_',' ').title()} Operation Performance")
    
    plt.xscale("log")                               # Use logarithmic scaling for x-axis
    
    # Set the dataset sizes used in experiment
    dataset_sizes = [100, 1000, 10000, 50000, 100000]
    plt.xticks(dataset_sizes)

    # Include thousands separators for clarity
    plt.gca().xaxis.set_major_formatter(
        ticker.FuncFormatter(lambda x, pos: f"{int(x):,}")
    )
    
    # Display legend and gridlines
    plt.legend(title="Database", loc="upper left", bbox_to_anchor=(1, 1))
    plt.grid(True)

    # Save the generated graph to graphs folder
    filename = f"graphs/{op}_performance.png"
    plt.tight_layout()
    plt.savefig(filename, dpi=600)

    # Print confirmation and close the figure before generating next graph
    print(f"Saved {filename}")
    plt.close()
    