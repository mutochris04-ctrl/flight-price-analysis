"""Task 1 - Ingestion: load a copy of the CSV into the MySQL staging table raw_flights.

The column names and data types are standardized on the way in, so the data is easier to process.
"""
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from src.db import get_mysql_engine

CSV_FILE = Path(__file__).resolve().parents[1] / "data" / "Flight_Price_Dataset_of_Bangladesh.csv"

# CSV column name -> column name in the database (short, no spaces).
# Both spellings of the fare columns are accepted ("Base Fare" and "Base Fare (BDT)").
COLUMN_NAMES = {
    "Airline": "airline",
    "Source": "source",
    "Source Name": "source_name",
    "Destination": "destination",
    "Destination Name": "destination_name",
    "Departure Date & Time": "departure_date_time",
    "Arrival Date & Time": "arrival_date_time",
    "Duration (hrs)": "duration_hrs",
    "Stopovers": "stopovers",
    "Aircraft Type": "aircraft_type",
    "Class": "travel_class",
    "Booking Source": "booking_source",
    "Base Fare": "base_fare",
    "Base Fare (BDT)": "base_fare",
    "Tax & Surcharge": "tax_surcharge",
    "Tax & Surcharge (BDT)": "tax_surcharge",
    "Total Fare": "total_fare",
    "Total Fare (BDT)": "total_fare",
    "Seasonality": "seasonality",
    "Days Before Departure": "days_before_departure",
}

# The columns of raw_flights (see sql/mysql_setup.sql), without the id
TABLE_COLUMNS = ["airline", "source", "source_name", "destination", "destination_name",
                 "departure_date_time", "arrival_date_time", "duration_hrs", "stopovers",
                 "aircraft_type", "travel_class", "booking_source", "base_fare",
                 "tax_surcharge", "total_fare", "seasonality", "days_before_departure"]

NUMBER_COLUMNS = ["duration_hrs", "base_fare", "tax_surcharge", "total_fare", "days_before_departure"]
DATE_COLUMNS = ["departure_date_time", "arrival_date_time"]


def read_csv(csv_file=CSV_FILE):
    """Read the CSV, rename the columns and give each column the right type."""
    df = pd.read_csv(csv_file)
    print("Read", len(df), "rows from", csv_file.name)

    df.columns = [column.strip() for column in df.columns]  # remove extra spaces in headers
    df = df.rename(columns=COLUMN_NAMES)

    # Every CSV column must have a place in the staging table
    unknown = [column for column in df.columns if column not in TABLE_COLUMNS]
    if unknown:
        raise ValueError("These CSV columns are not in raw_flights: " + str(unknown)
                         + " - add them to COLUMN_NAMES and sql/mysql_setup.sql")

    # A column that is missing in the CSV is stored as empty (NULL)
    for column in TABLE_COLUMNS:
        if column not in df.columns:
            print("Warning: column", column, "is not in the CSV - it will be empty")
            df[column] = None

    # Numbers: text that is not a number becomes empty, so validation can find it
    for column in NUMBER_COLUMNS:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    for column in DATE_COLUMNS:
        df[column] = pd.to_datetime(df[column], errors="coerce")

    return df[TABLE_COLUMNS]


def ingest_data():
    """Empty the staging table, insert all CSV rows, and check that nothing was lost."""
    df = read_csv()
    engine = get_mysql_engine()

    with engine.begin() as connection:
        connection.execute(text("TRUNCATE TABLE raw_flights"))  # no duplicates if the DAG runs again
        df.to_sql("raw_flights", connection, if_exists="append", index=False,
                  chunksize=1000, method="multi")
        rows_in_mysql = connection.execute(text("SELECT COUNT(*) FROM raw_flights")).scalar()

    print("Rows in CSV:", len(df), "| rows in MySQL raw_flights:", rows_in_mysql)
    if rows_in_mysql != len(df):
        raise ValueError("Not all rows were inserted into MySQL!")
    print("Ingestion OK")


if __name__ == "__main__":
    ingest_data()
