# Flight Price Analysis with Apache Airflow

An end-to-end data pipeline for Bangladesh flight prices.
Airflow runs six tasks in order: wait until the CSV has arrived, load it into **MySQL** (staging),
validate and clean it, transform it, calculate four KPIs, and load the results into **PostgreSQL** (analytics).

```
CSV -> [wait] -> [ingest] -> MySQL raw_flights -> [validate] -> [transform] -> [KPIs] -> [load] -> PostgreSQL
```

## Project structure

```
dags/flight_price_dag.py   the Airflow DAG: the 6 tasks and their order
src/db.py                  database connections (settings come from .env)
src/inspect_data.py        first look at the CSV (run once by hand)
src/ingest_data.py         Task 1: a copy of the CSV -> MySQL raw_flights (names and types standardized), with a row-count check
src/validate_data.py       Task 2: required columns, text cleaning, invalid rows -> rejected_flights
src/transform_data.py      Task 3: total fare check, route, peak / non-peak season (known labels only)
src/calculate_kpis.py      Task 4: the four KPIs
src/load_data.py           Task 5: MySQL -> PostgreSQL in one transaction
src/alerts.py              retry and failure alerts -> data/alerts.log (+ Slack if set)
src/mini_test_demo.py      mini test: runs my validation + transformation on 11 test rows
data/mini_test_flights.csv 11 made-up flights, each one tests a different problem
sql/mysql_setup.sql        MySQL staging table with column types
sql/postgres_setup.sql     PostgreSQL tables for the flights and KPIs
docker-compose.yml         MySQL + PostgreSQL + Airflow containers
REPORT.md                  architecture, tasks, KPI logic, results, challenges
```

## Setup

1. Install **Docker Desktop** and start it.
2. Download `Flight_Price_Dataset_of_Bangladesh.csv` from Kaggle and put it in the `data/` folder.
3. Copy `.env.example` to `.env` and choose your passwords.
4. Start the containers (the first time takes a few minutes):
   ```
   docker compose up -d
   ```
5. Get the Airflow password (user name: `admin`):
   ```
   docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
   ```
6. Open http://localhost:8080 and log in.

## Run the pipeline

In the Airflow page, open **flight_price_analysis** and click **Trigger DAG** (the ▶ button).
Each task turns green when it succeeds. Click a task, then **Logs**, to see what it did.

The first task, `wait_for_csv`, checks every minute whether the CSV is in `data/` and not empty.
It waits up to 2 hours; if the file never arrives, it fails and the other tasks do not run.

A single step can also be run by hand, for testing:
```
docker compose exec airflow python -m src.ingest_data
```

## Mini test (prove the checks work)

In the real dataset every row passes validation, so I made a small test file with 11 made-up flights,
each with a different problem (negative fare, missing total, duplicate, wrong total, missing airline,
wrong airport code, same source and destination, unknown season label). This runs my real validation and transformation code
on it and prints what happened. Nothing is saved in the databases.
```
docker compose exec airflow python -m src.mini_test_demo
```
Expected result: 6 valid rows, 5 rejected rows, 1 fare mismatch, and 1 unknown season left empty.

## Failure test (prove retries and alerts work)

Every task retries 2 times (1 minute apart) and may work 10 minutes at most.
Airflow calls my alert functions (`src/alerts.py`) by itself: a RETRY line after each failed try,
and one ALERT line after the last try. They go to `data/alerts.log`, and to Slack if
`SLACK_WEBHOOK_URL` is set in `.env`.

1. In `src/load_data.py`, remove the `#` in front of `raise RuntimeError("Test failure: PostgreSQL is down!")`.
2. Trigger the DAG. `load_to_postgres` goes yellow (retry), yellow (retry), then red. The other tasks stay green.
3. Show the alerts:
   ```
   Get-Content data\alerts.log
   ```
   Expected: 2 RETRY lines and 1 ALERT line.
4. Put the `#` back, then in the UI click the red `load_to_postgres` square and **Clear**: only that task runs again and turns green.

## Check the results

MySQL (Workbench: host `localhost`, port **3307**, user `root`):
```sql
USE flight_staging;
SELECT COUNT(*) FROM raw_flights;
SELECT problem, COUNT(*) FROM rejected_flights GROUP BY problem;
```

PostgreSQL (pgAdmin: host `localhost`, port **5433**, database `flight_analytics`):
```sql
SELECT COUNT(*) FROM flights;
SELECT * FROM kpi_airline ORDER BY avg_total_fare DESC;
SELECT * FROM kpi_peak;
SELECT * FROM kpi_route ORDER BY route_rank;
SELECT fare_mismatch, COUNT(*) FROM flights GROUP BY fare_mismatch;
```

From PowerShell, without pgAdmin:
```
docker compose exec postgres psql -U postgres -d flight_analytics -c "SELECT COUNT(*) FROM flights;"
```
Run the DAG twice: the count must still be 57000 (the pipeline replaces data, it does not add it again).

## Stop

```
docker compose down        # stops the containers, keeps the data
docker compose down -v     # stops them and deletes the database data
```

## Results

See [REPORT.md](REPORT.md).
