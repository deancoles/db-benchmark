## Database Language Benchmark (SQL vs NoSQL)

This project supports my dissertation, which compares SQL and NoSQL database languages, and challenges the consensus in literature, textbooks, and vendor documentation that each language has fixed strengths, weaknesses, and use cases.  

The benchmarking app uses representative systems to capture both language families and their practical paradigms:

- SQLite → lightweight applications (e.g., mobile, embedded, local apps)  
- MySQL → transaction-heavy relational systems (e.g., banking, e-commerce)  
- MongoDB → large unstructured storage (e.g., social media, IoT, big data)  
- Redis → key–value store for ultra-fast lookups (e.g., caching, session management, real-time analytics)

Two systems are taken from each language family to reflect different usage contexts:
- SQL → SQLite (embedded) and MySQL (server)
- NoSQL → MongoDB (document) and Redis (key–value)

### Project Overview
I am developing a Python benchmarking app that:

- Generates small to large catalogue-style datasets
- Loads them into each system via a common adaptor interface
- Runs CRUD workloads (insert, read, update, delete)
- Records timings/results to CSV for analysis
- Supports cold vs warm runs using a reset flag (warm = reuse data, cold = reset data)

This application is designed solely to generate comparative performance data for my dissertation; it is not intended as a production tool.

### Repository Structure
- adaptors/ → DB-specific adaptors
    - sqlite_adaptor.py → embedded SQL
    - mysql_adaptor.py → server SQL
    - mongodb_adaptor.py → document NoSQL
    - redis_adaptor.py → key–value NoSQL
- datasets/ → dataset generator(s) and test data
- results/ → CSV outputs and plots (analysis)
- docs/ → screenshots and development logs
- runner.py → selects adaptor by DB_TYPE and runs one CRUD cycle



