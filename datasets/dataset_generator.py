"""
dataset_generator.py
---------------------

Purpose:
    This file will generate artificial catalogue-style datasets for benchmarking
    SQL, NoSQL, and SQLite.

Details:
    - Datasets will include fields such as id, title, year, and tags.
    - Sizes will range from 100 to 100,000 records.
    - Data will be generated randomly but consistently using a fixed seed.

Author: Dean Coles
Date: 2025-08-25
"""

def generate_dataset(size=100):
    """
    Placeholder dataset generator function.

    Args:
        size (int): Number of records to generate (default = 100).

    Returns:
        list: A placeholder list of records.
    """
    return [f"Record {i+1}" for i in range(size)]


if __name__ == "__main__":
    # Quick test run
    sample = generate_dataset(5)
    print(sample)
