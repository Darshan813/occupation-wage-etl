# Occupation Wage ETL (OEWS + O*NET)

This repository contains an end-to-end ETL pipeline that ingests U.S. Bureau of Labor Statistics Occupational Employment and Wage Statistics (OEWS) data and enriches it with O*NET skills, producing date-partitioned Parquet outputs and loading cleaned data into PostgreSQL. The pipeline is orchestrated with Apache Airflow (Docker), with helper notebooks for exploratory analysis.

## What’s inside

- airflow_pipeline/
  - docker-compose.yaml — Airflow stack (webserver, scheduler, etc.)
  - .env — Environment configuration for Airflow stack
  - dags/oews_onet_dag.py — Main Airflow DAG orchestrating the pipeline
  - pipeline/load_data.py — ETL logic used by tasks
  - data/ — Dated partitions of raw/cleaned outputs (Parquet)
    - oews_raw/ YYYY-MM-DD/
    - oews_cleaned/ YYYY-MM-DD/
    - onet_skills_raw/ YYYY-MM-DD/
    - onet_skills_cleaned/ YYYY-MM-DD/
    - Skills.xlsx — Input workbook for O*NET skills
  - config/airflow.cfg — Optional Airflow overrides
  - logs/ — Airflow task logs by run
- notebooks/
  - analysis.ipynb — Exploration and ad‑hoc analysis


## Quickstart

Prerequisites:
- Docker Desktop (Windows/macOS/Linux)
- Docker Compose v2
- Python 3.10+ (optional, for local notebook usage)

```powershell
# Windows PowerShell
docker compose up -d
```

Defaults (as commonly used):
- Pgadmin: port 8081 → 127.0.0.1:8081
    - user_id - admin@admin.com
    - password - admin
    - to connect to the server
        use host - db, username, admin, password - admin
- Airflow: port 8080 → http://localhost:8080
    - user - airflow
    - password - airflow

### 2) Launch Airflow stack

# Start Airflow
docker compose up -d

Then open the Airflow UI (check the port in your `airflow_pipeline/docker-compose.yaml`; typical is http://localhost:8080). Log in with the credentials configured in your .env (or defaults in the compose file).

Enable and run the DAG
- In Airflow UI, enable `oews_onet_pipeline` (or the DAG name shown in `dags/oews_onet_dag.py`).
- Trigger a manual run, or let the schedule run it (if a schedule is defined).
- Check task logs under `airflow_pipeline/logs/` or from the UI for progress.

Outputs are written under `airflow_pipeline/data/` in date-stamped folders:
- `oews_raw/YYYY-MM-DD/`
- `oews_cleaned/YYYY-MM-DD/`
- `onet_skills_raw/YYYY-MM-DD/`
- `onet_skills_cleaned/YYYY-MM-DD/`

## Important files
- `airflow_pipeline/dags/oews_onet_dag.py` — DAG definition and task orchestration
- `airflow_pipeline/pipeline/load_data.py` — Core ETL steps (read, clean, transform, write)
- `airflow_pipeline/data/` — Partitioned outputs (Parquet) for raw and cleaned datasets
- `airflow_pipeline/Skills.xlsx` — Source workbook with O*NET skills used in enrichment
- `airflow_pipeline/.env` — Airflow environment configuration
- `airflow_pipeline/docker-compose.yaml` — Airflow services (webserver, scheduler, etc.)
- `notebooks/analysis.ipynb` — Exploratory analysis and ad‑hoc data prep

## Development notes
- Be mindful of port conflicts (pgAdmin uses 8081; Airflow use 8080).
- If you change Postgres credentials in `docker-compose.yaml` after data has been created, the existing volume will keep the old password. Either reset the password inside Postgres or remove the volume to reinitialize.

## Troubleshooting

- SQLAlchemy: `Not an executable object: 'SELECT ...'`
  - Use `from sqlalchemy import text` and then `conn.execute(text("SELECT ..."))` or `conn.exec_driver_sql("SELECT ...")`.

- Postgres: `password authentication failed`
  - The password in your running Postgres may not match the environment value because the data directory (`pgdata` volume) is already initialized. Reset the role password via `psql` or remove the volume: `docker compose down -v` (destructive!) and recreate.

- Airflow cannot reach Postgres
  - If Postgres is not in the same compose network, use `host.docker.internal` as the host and the published port (e.g., 5432). Ensure the port is exposed in the Postgres compose file.

- Port already in use
  - Adjust the published ports in compose files (e.g., change pgAdmin or Airflow port) or stop the conflicting service.
