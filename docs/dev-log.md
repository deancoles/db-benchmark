\## 2025-08-24

\- Created initial project structure (folders + .gitkeep).

\- Added full README with project overview.

\- First commit: "Initial structure".


\## 2025-08-24

\- Created runner.py with placeholder main() function and docstring.

\- Verified that the file runs successfully and prints a message.


\## 2025-08-25

\- Added `datasets/dataset\_generator.py` with a docstring and placeholder function.  

\- Function currently generates a list of sample records (e.g., “Record 1”, “Record 2”).  

\- Tested script by running it directly, confirmed it prints out sample data.  

\- Removed `.gitkeep` from `datasets/` and `docs/` since these folders now contain real files.  

\- Verified repo structure looks clean and functional.


\## 2025-08-25

\- Created `adaptors/sqlite_adaptor.py`.

\- Added docstring describing purpose and planned functionality.

\- Implemented `connect()` function to establish SQLite connection.

\- Added placeholder functions for insert, read, update, and delete.

\- Removed `.gitkeep` from `adaptors/` since folder now has a real file.

\- Verified script connects to `benchmark.db` and closes successfully.


\## 2025-08-26

\- Expanded `adapters/sqlite_adaptor.py` with basic CRUD functions.

\- Added insert, read, update, and delete methods.

\- Test block now creates a table, inserts sample records, updates one, and deletes another.

\- Verified script runs successfully and outputs correct results.

\- Saved screenshot: 2025-08-26_sqlite_adaptor_crud.png


## 2025-08-26

\- Created `adaptors/mysql_adaptor.py` with full CRUD functions.  

\- Implemented connect() using mysql-connector-python.  

\- Added functions for table creation, insert, read, update, and delete.  

\- Configured credentials to load from environment variables with python-dotenv.  

\- Confirmed `.env` is ignored by Git to protect sensitive data.  

\- Verified script connects to MySQL `benchmark` database and runs CRUD operations successfully.  

\- Saved screenshot: 2025-08-26_mysql_adaptor_crud.png  



