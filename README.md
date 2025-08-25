## Database Language Benchmark (SQL, NoSQL, SQLite)

This project is part of my dissertation exploring and challenging the consensus in literature, textbooks, and vendor documentation that each language has its own best use case:

- SQLite → lightweight applications (e.g., mobile, embedded, local apps)  
- SQL → transaction-heavy relational systems (e.g., banking, e-commerce)  
- NoSQL → large unstructured storage (e.g., social media, IoT, big data)  

### Project Overview
I am developing a Python benchmarking app with a Tkinter interface that:
- Generates catalogue-style datasets  
- Loads them into each language  
- Runs workloads such as insert, read, update, and delete  
- Records results in CSV format for analysis  

### Repository Structure
- `adapters/` → DB-specific adapters (SQLite, MySQL, MongoDB)  
- `datasets/` → generated input data  
- `results/` → benchmark results (CSV, plots)  
- `docs/` → screenshots and development log  








