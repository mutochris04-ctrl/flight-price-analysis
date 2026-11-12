"""Task 5 - Loading: copy the clean flights and the KPI tables from MySQL to PostgreSQL."""
import pandas as pd
from sqlalchemy import text

from src.db import get_mysql_engine, get_postgres_engine

# MySQL staging table -> PostgreSQL analytics table
TABLES = {
    "transformed_flights": "flights",
    "kpi_airline": "kpi_airline",
    "kpi_season": "kpi_season",
    "kpi_peak": "kpi_peak",
    "kpi_route": "kpi_route",
}


def load_data():
    """Load every table in ONE transaction: either everything is saved, or nothing is."""
    # FAILURE TEST (to show retries and alerts): remove the # on the next line, then trigger the DAG.
    # raise RuntimeError("Test failure: PostgreSQL is down!")

    mysql_engine = get_mysql_engine()
    postgres_engine = get_postgres_engine()

    # 1. Read everything from MySQL first
    data = {}
    for mysql_table, postgres_table in TABLES.items():
        data[postgres_table] = pd.read_sql("SELECT * FROM " + mysql_table, mysql_engine)

    # MySQL stores True/False as 1/0; PostgreSQL needs real booleans
    data["flights"]["fare_mismatch"] = data["flights"]["fare_mismatch"].astype(bool)

    # 2. Replace the PostgreSQL data in one transaction
    with postgres_engine.begin() as connection:
        connection.execute(text("TRUNCATE TABLE " + ", ".join(data.keys())))
        for table, df in data.items():
            df.to_sql(table, connection, if_exists="append", index=False,
                      chunksize=1000, method="multi")

        # 3. Check the row counts before the transaction is committed
        for table, df in data.items():
            rows = connection.execute(text("SELECT COUNT(*) FROM " + table)).scalar()
            print(table + ":", rows, "rows")
            if rows != len(df):
                raise ValueError("Row count is wrong for " + table + " - nothing was saved")

    print("Loading into PostgreSQL OK")


if __name__ == "__main__":
    load_data()
