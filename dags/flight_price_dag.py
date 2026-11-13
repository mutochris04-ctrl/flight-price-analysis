"""Airflow DAG: Bangladesh flight price pipeline (CSV -> MySQL -> validate -> KPIs -> PostgreSQL)."""
# ----------------------------------------------------------------------
# THE LOGICAL ORDER OF MY LAB - ELTL = Extract, Load, Transform, Load
#   Step 0  wait_for_csv         -> wait until the CSV file has arrived
#   Step 1  ingest_csv_to_mysql  -> E + L : Extract the CSV, Load it RAW into MySQL (staging)
#   Step 2  validate_data        -> T     : Transform part 1 - good rows / bad rows
#   Step 3  transform_data       -> T     : Transform part 2 - total fare, route, peak season
#   Step 4  calculate_kpis       -> T     : Transform part 3 - the 4 KPIs
#   Step 5  load_to_postgres     -> L     : Load the final results into PostgreSQL (analytics)
# Data path: CSV -> raw_flights -> validated_flights -> transformed_flights -> kpi_* tables
#            (all in MySQL)  -> then copied to PostgreSQL
# ----------------------------------------------------------------------
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from src.ingest_data import ingest_data, CSV_FILE
from src.validate_data import validate_data
from src.transform_data import transform_data
from src.calculate_kpis import calculate_kpis
from src.load_data import load_data
from src.alerts import notify_retry, alert_on_failure


# Task 0 helper: is the data here yet?
def csv_is_ready():
    """Return True when the CSV file exists and is not empty. Airflow keeps asking until it is True."""
    if not os.path.exists(CSV_FILE):
        print("Waiting: the CSV file has not arrived yet:", CSV_FILE)
        return False
    if os.path.getsize(CSV_FILE) == 0:
        print("Waiting: the CSV file is there but empty")
        return False
    print("The CSV file is ready")
    return True


# Settings for every task
default_args = {
    "owner": "christine",
    "retries": 2,                         # try again if a task fails (2 retries = 3 attempts in total)
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),   # a task working more than 10 minutes fails (the whole DAG takes about 1.5 minutes)
    "on_retry_callback": notify_retry,            # after attempt 1 and 2 fail: write a RETRY line
    "on_failure_callback": alert_on_failure,      # once, after the last attempt fails: send the ALERT
}

# Documentation shown in the Airflow UI (DAG page)
DAG_DOC = """
### Flight Price Analysis
Loads the Bangladesh flight price CSV into MySQL staging, validates and transforms it,
calculates 4 KPIs and loads everything into PostgreSQL.

* **Owner:** christine
* **Run:** by hand (Trigger button); `wait_for_csv` waits up to 2 hours for the CSV.
* **Retries:** 2 per task (1 minute apart). Each task may work 10 minutes at most.
* **Alerts:** every retry and final failure is written to `data/alerts.log` (and sent to Slack if `SLACK_WEBHOOK_URL` is set).
* **Safe to re-run:** every task replaces its output table, so there are no duplicates.
"""

with DAG(
    dag_id="flight_price_analysis",
    description="Ingest, validate, transform and analyse Bangladesh flight prices",
    start_date=datetime(2026, 11, 1),
    schedule=None,                        # run by hand with the Trigger button
    catchup=False,                        # do not run old missed dates
    default_args=default_args,
    tags=["lab", "flights"],
    doc_md=DAG_DOC,
) as dag:

    # Task 0: wait for the data. Check every minute, give up after 2 hours.
    wait_for_csv = PythonSensor(
        task_id="wait_for_csv",
        python_callable=csv_is_ready,
        poke_interval=60,          # check every 60 seconds
        timeout=2 * 60 * 60,       # stop waiting after 2 hours (the task fails)
        mode="reschedule",         # free the worker between checks
    )

    ingest = PythonOperator(task_id="ingest_csv_to_mysql", python_callable=ingest_data)
    validate = PythonOperator(task_id="validate_data", python_callable=validate_data)
    transform = PythonOperator(task_id="transform_data", python_callable=transform_data)
    kpis = PythonOperator(task_id="calculate_kpis", python_callable=calculate_kpis)
    load = PythonOperator(task_id="load_to_postgres", python_callable=load_data)

    # The order of the tasks
    # Step 0 -> 1 -> 2 -> 3 -> 4 -> 5: a task starts only when the task before it succeeded
    wait_for_csv >> ingest >> validate >> transform >> kpis >> load
