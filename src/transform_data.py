"""Task 3 - Transformation: total fare, route and season type for every valid flight."""
import pandas as pd

from src.db import get_mysql_engine

# The season labels in the dataset (written in small letters, so "Eid" and "eid" both match)
NON_PEAK_SEASONS = ["regular"]
PEAK_SEASONS = ["winter holidays", "hajj", "eid"]


def season_from_month(date):
    """Backup rule if the data has no season: December and January are winter holidays."""
    if pd.isnull(date):
        return pd.NA
    if date.month in [12, 1]:
        return "Winter Holidays"
    return "Regular"


def transform_dataframe(df):
    """Add calculated_total_fare, fare_mismatch, route and season_type."""
    # Total Fare = Base Fare + Tax & Surcharge
    df["calculated_total_fare"] = (df["base_fare"] + df["tax_surcharge"]).round(2)

    # If the total is missing, use the calculation
    missing_total = df["total_fare"].isnull()
    df.loc[missing_total, "total_fare"] = df.loc[missing_total, "calculated_total_fare"]
    print("Total fare calculated for", int(missing_total.sum()), "rows where it was missing")

    # If the total is present but different, keep it and flag it (more than 1 BDT = not rounding)
    df["fare_mismatch"] = (df["total_fare"] - df["calculated_total_fare"]).abs() > 1
    print("Rows where total fare is different from base + tax:", int(df["fare_mismatch"].sum()))

    # Route, e.g. DAC-CXB
    df["route"] = df["source"] + "-" + df["destination"]

    # Season: use the dataset column; only if it is completely empty, use the month rule
    if df["seasonality"].isnull().all():
        print("No seasonality in the data - using the month rule (December/January = winter)")
        df["seasonality"] = pd.to_datetime(df["departure_date_time"]).apply(season_from_month)

    # Peak or Non-Peak, only for the season labels I know.
    # A missing or unknown label stays empty, so it is never counted as Peak by mistake.
    season = df["seasonality"].str.lower()
    df["season_type"] = pd.NA
    df.loc[season.isin(PEAK_SEASONS), "season_type"] = "Peak"
    df.loc[season.isin(NON_PEAK_SEASONS), "season_type"] = "Non-Peak"

    unknown = df["seasonality"].notnull() & df["season_type"].isnull()
    if unknown.sum() > 0:
        print("Warning:", int(unknown.sum()), "rows have an unknown season label:",
              list(df.loc[unknown, "seasonality"].unique()), "- season_type left empty")

    return df


def transform_data():
    """Read validated_flights, transform it, and save transformed_flights in MySQL."""
    engine = get_mysql_engine()
    df = pd.read_sql("SELECT * FROM validated_flights", engine)

    df = transform_dataframe(df)

    df.to_sql("transformed_flights", engine, if_exists="replace", index=False, chunksize=1000)
    print("Saved", len(df), "rows to transformed_flights")


if __name__ == "__main__":
    transform_data()
