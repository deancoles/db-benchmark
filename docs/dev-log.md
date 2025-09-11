\## 24-08-2025

\- Created initial project structure (folders + .gitkeep).

\- Added full README with project overview.

\- First commit: "Initial structure".


\## 24-08-2025

\- Created runner.py with placeholder main() function and docstring.

\- Verified that the file runs successfully and prints a message.


\## 25-08-2025

\- Added `datasets/dataset\_generator.py` with a docstring and placeholder function.  

\- Function currently generates a list of sample records (e.g., “Record 1”, “Record 2”).  

\- Tested script by running it directly, confirmed it prints out sample data.  

\- Removed `.gitkeep` from `datasets/` and `docs/` since these folders now contain real files.  

\- Verified repo structure looks clean and functional.


\## 25-08-2025

\- Created `adaptors/sqlite_adaptor.py`.

\- Added docstring describing purpose and planned functionality.

\- Implemented `connect()` function to establish SQLite connection.

\- Added placeholder functions for insert, read, update, and delete.

\- Removed `.gitkeep` from `adaptors/` since folder now has a real file.

\- Verified script connects to `benchmark.db` and closes successfully.


\## 26-08-2025

\- Expanded `adaptors/sqlite_adaptor.py` with basic CRUD functions.

\- Added insert, read, update, and delete methods.

\- Test block now creates a table, inserts sample records, updates one, and deletes another.

\- Verified script runs successfully and outputs correct results.

\- Saved screenshot: 2025-08-26_sqlite_adaptor_crud.png


## 26-08-2025

\- Created `adaptors/mysql_adaptor.py` with full CRUD functions.  

\- Implemented connect() using mysql-connector-python.  

\- Added functions for table creation, insert, read, update, and delete.  

\- Configured credentials to load from environment variables with python-dotenv.  

\- Confirmed `.env` is ignored by Git to protect sensitive data.  

\- Verified script connects to MySQL `benchmark` database and runs CRUD operations successfully.  

\- Saved screenshot: 2025-08-26_mysql_adaptor_crud.png  


## 26-08-2025

\- Created `adaptors/mongodb_adaptor.py` with CRUD functions.  

\- Implemented `connect()` using pymongo and environment variables.  

\- Added insert, read, update, and delete operations with a test block.  

\- Verified output shows correct changes after each operation.  

\- Checked results in MongoDB Compass for confirmation.  

\- Saved screenshot: 2025-08-26_mongodb_adaptor_crud.png  


## 26-08-2025

\- Integrated `runner.py` with environment-driven DB_TYPE switching.  

\- Added RESET_DATA flag to allow clean slate benchmarking runs.  

\- Implemented `reset_table()` in SQLite and MySQL adaptors.  

\- Ran and verified CRUD operations across SQLite, MySQL, and MongoDB.  

\- Captured screenshots showing `runner.py` alongside terminal output for each database.  


## 27-08-2025

\- Added `redis_adaptor.py` to support Memurai/Redis as a key–value NoSQL database.  

\- Updated `runner.py` to include Redis branch with full CRUD cycle.  

\- Extended `.env` with Redis configuration variables.  

\- Enhanced `sqlite_adaptor.reset_table()` to also reset AUTOINCREMENT counter.  

\- Modified `mongodb_adaptor` to use a numeric `seq` field for CRUD, aligning behaviour with SQL/Redis.  

\- Adjusted `runner.py` Mongo branch to update/delete by `seq` values instead of names.  

\- Added non-unique index on Mongo seq field to allow repeated inserts during benchmarks.  

\- Standardised all adaptor test blocks so they always reset state when run directly.  

\- Updated adaptor docstrings with clear notes about reset behaviour for manual tests.


## 28-08-2025

\- Added `utils/benchmark_utils.py` with helpers to time operations and summarise results.  

\- Integrated timing into `runner.py` for SQLite, MySQL, MongoDB, and Redis CRUD blocks.  

\- Each operation now reports mean, median, min, and max across repeated runs.  

\- Verified SQLite benchmark run with 3 records repeated 5 times (total 15 inserts).  

\- Output confirmed CRUD results and timing summaries printed correctly.  

\- Saved screenshot: 2025-08-28_sqlite_benchmark_run.png


## 09-09-2025

\- Added `.env` settings: `REPEATS` (repeat count) and `DATASET_SIZE` (dataset size).

\- Updated `benchmark_utils.print_summary_line` to multi-line format with 3 decimal places for readability.

\- Implemented `write_summary_csv` to save results to `/results/` with 6 decimal places for precision.

\- Updated `runner.py` to call CSV writer after each CRUD summary.

\- CSV filenames now include date, database type, run type (cold/warm), and dataset size.

\- Verified functionality with SQLite, MySQL, MongoDB, and Redis runs.

\- Observed that cold runs were sometimes faster than warm runs at very small dataset sizes (e.g. 5 records). Plan to re-test at larger sizes to confirm caching effects.

\- Saved screenshots: `2025-09-09_sqlite_multiline_summary.png`, `2025-09-09_results_folder.png`


## 11-09-2025

\- Rewrote and aligned comments across all adaptor files for clarity.  

\- Standardised header format with *What this file does / Used by / Note*.  

\- Ensured demo comments and wording were consistent (e.g. newest row/document/key).  

\- Updated `runner.py` comments to reflect current Redis counter behaviour.  

\- Verified that comments are now consistent and make sense in their placement.  
