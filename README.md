# Idempotent Python ETL Pipeline (PostgreSQL & Docker)

A production-grade, fault-tolerant, and idempotent ETL pipeline built with Python, PostgreSQL, and Docker. The pipeline extracts data from a local CSV file and a REST API, cleans and joins them, and performs a batch upsert into PostgreSQL.

---

## 🏗 Architecture & Workflow

```text
[ CSV Data ] ---\
                 +---> [ Transform & Error Handling ] ---> [ Batch Upsert (ON CONFLICT) ] ---> [ PostgreSQL ]
[ REST API ] ---/            (Pandas Cleaning)                      (SQLAlchemy)

Extract: Reads user data sequentially from data/users_extra.csv and the JSONPlaceholder REST API (/users).

Transform: Validates data types, handles invalid entries gracefully, drops nulls, and performs an inner join on user_id.

Load: Executes a bulk upsert (INSERT ... ON CONFLICT (user_id) DO UPDATE) into PostgreSQL to guarantee idempotence.

Logging & Error Handling: Fully relies on Python's logging library. Skips malformed rows without interrupting the execution flow.

🛠 Tech Stack
Language: Python 3.11

Data Handling: Pandas, Requests

Database: PostgreSQL 15, SQLAlchemy, Psycopg2

Containerization: Docker, Docker Compose

🚀 How to Run
Prerequisites
Docker & Docker Compose installed on your system.

Quick Start
Clone the repository and run the entire pipeline with a single command:
docker-compose up --build

Verifying Results
To verify the loaded records in PostgreSQL:
docker exec -it etl_postgres psql -U postgres -d etl_db -c "SELECT * FROM users_etl;"

Idempotency Check
Run the app container a second time to confirm that no duplicate records are created and the total row count remains unchanged (9 rows):
docker-compose run --rm etl_app

