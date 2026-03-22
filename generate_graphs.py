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
plt.rcParams.update({"font.size": 11})              # Font size

# Benchmark operations that will each receive a graph
operations = [
    "insert",
    "full_scan",
    "lookup",
    "update",
    "delete"
]
database_order = ["sqlite", "mysql", "mongodb", "redis"]                    # Fixed order for consistency
databases = [db for db in database_order if db in df["db"].unique()]        # Identify unique database systems
dataset_sizes = [10000, 50000, 100000, 250000, 500000]                      # Dataset sizes

# Generate a graph for each operation type
for op in operations:
    subset = df[df["operation"] == op]                                      # Filter dataset to only include current operation
    
    # Skip empty operations safely
    if subset.empty:
        print(f"Skipped {op}: no matching data found")
        continue
    
    plt.figure(figsize=(12,5))                                              # Create new figure for the current operation
    
    markers = ["o", "s", "^", "D"]

    # Plot a line for each database system
    for i, db in enumerate(databases):

        # Extract data for current database and order by record count (prevents lines appearing out of order on graph)
        db_data = subset[subset["db"] == db].sort_values("records")
        
        if db_data.empty:
            continue

        # Plot execution time against dataset size
        plt.plot(
            db_data["records"],
            db_data["mean_time"],
            marker=markers[i],
            linewidth=1.8,
            label=db
        )

    # Axis labels and graph title
    plt.title(f"{op.replace('_',' ').title()} Operation Performance")
    plt.xlabel("Dataset Size (records)")
    plt.ylabel("Execution Time (s)")
    
    # Make x-axis readable
    plt.xticks(dataset_sizes)
    plt.gca().xaxis.set_major_formatter(
        ticker.FuncFormatter(lambda x, pos: f"{int(x):,}" if x >= 1 else "")
    )

    # Format y-axis as normal decimal seconds
    plt.gca().yaxis.set_major_formatter(
        ticker.FormatStrFormatter("%.3f")
    )

    # Display legend and gridlines
    plt.grid(True,linestyle="-", alpha=0.5)      
    plt.legend(title="Database", loc="upper left", bbox_to_anchor=(1.02, 1))


    # Save the generated graph to graphs folder
    filename = f"graphs/{op}_performance.png"
    plt.tight_layout(rect=[0, 0, 0.85, 1])
    plt.savefig(filename, dpi=600)

    # Print confirmation and close the figure before generating next graph
    print(f"Saved {filename}")
    plt.close()
    