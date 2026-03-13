## Database Language Benchmark (SQL vs NoSQL)

This project supports my dissertation research, which evaluates performance differences between SQL and NoSQL database languages through controlled benchmarking experiments.
The study investigates whether commonly reported advantages of relational and non-relational database models remain consistent when tested under identical workloads and experimental conditions.
Four representative database systems are used to reflect different architectural approaches to data management.



### Systems Compared

The benchmarking framework evaluates four widely used database systems:



       Database				Model								    Typical Usage
	SQLite		    	Relational (Embedded)				Mobile applications, embedded systems
	MySQL		    	Relational (Client–Server)			Transactional systems, enterprise databases
	MongoDB		Document (NoSQL)					Large-scale unstructured data
	Redis		    	Key–Value (NoSQL)					Caching, session storage, real-time analytics



Two systems represent relational databases and two represent NoSQL architectures, allowing comparison between both language families.



### Project Overview

The benchmarking application is written in Python and executes identical workloads across each database system.



The framework performs the following tasks:

* Generates catalogue-style datasets of varying sizes
* Loads data into each database through a common adaptor interface
* Executes CRUD operations (insert, read, update, delete)
* Records execution times for each operation
* Outputs results to CSV files for analysis



Cold and warm benchmarking runs are supported through a reset flag that controls whether data is regenerated or reused.



### Repository Structure

adaptors/       			Database-specific adaptors
datasets/       			Dataset generation utilities
results/        			Benchmark output files
docs/           			Development logs and screenshots

runner.py       			Main benchmark runner
generate\_graphs.py  	Graph generation script
requirements.txt 		Python dependencies



### Installation

### 1\. Clone the repository

git clone https://github.com/deancoles/database-language-benchmark.git cd database-language-benchmark

### 2\. Create a virtual environment

python -m venv venv



Activate it:

Windows

venv\\Scripts\\activate

### 3\. Install dependencies

pip install -r requirements.txt



### Environment Configuration

Create a .env file in the project root containing database connection details.



Example:

MYSQL\_HOST=localhost
MYSQL\_USER=root
MYSQL\_PASSWORD=password
MYSQL\_DATABASE=benchmark\_db

MONGODB\_URI=mongodb://localhost:27017



REDIS\_HOST=localhost
REDIS\_PORT=6379



### Running Benchmarks

To run the benchmarking experiment:

python runner.py



The script will:

1. Generate test datasets
2. Load data into the selected database
3. Execute CRUD workloads
4. Record timing results

Output files are written to the results/ directory.



### Generating Graphs

Benchmark results can be visualised using the graph generation script:

python generate\_graphs.py



This produces performance graphs for:

* Insert operations
* Full dataset scans
* Primary key lookups
* Update operations
* Delete operations

These graphs are used for analysis.



### Reproducibility

The benchmarking framework is designed to ensure experimental consistency by:

* Using identical datasets across all database systems
* Running identical CRUD workloads
* Recording results automatically to CSV
* Using controlled dataset sizes

The included requirements.txt file allows the Python environment to be recreated easily.



### Dissertation Context

This benchmarking tool forms the artefact component of a dissertation investigating the performance characteristics of SQL and NoSQL database languages.

The goal of the project is to empirically test whether commonly cited differences between relational and non-relational systems remain observable under controlled experimental conditions.

