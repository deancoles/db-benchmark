"""
dataset_generator.py
---------------------

What this file does:
    - Generates simple placeholder record values for benchmarking.
    - Produces datasets of different sizes for consistent CRUD testing.

Used by:
    - runner.py (to create the input dataset for each benchmark run)
"""

def generate_dataset(size=100):
    return [f"Record {i+1}" for i in range(size)]


if __name__ == "__main__":
    sample = generate_dataset(5)
    print(sample)
