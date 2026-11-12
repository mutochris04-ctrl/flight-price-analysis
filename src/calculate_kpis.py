"""Task 4 - KPIs: the four business measurements required by the lab."""
import pandas as pd

from src.db import get_mysql_engine

TOP_ROUTES = 10


def airline_kpi(df):
    """KPI 1 + KPI 3: average total fare and number of booking records for each airline.

    One row = one booking record (the dataset has no booking ID), so booking_count counts rows.
    """
    result = df.groupby("airline").agg(
        booking_count=("id", "count"),
        avg_total_fare=("total_fare", "mean"),
    ).reset_index()
    result["avg_total_fare"] = result["avg_total_fare"].round(2)
    return result.sort_values("avg_total_fare", ascending=False)


def season_kpi(df):
    """KPI 2a: average fare for each season, and how much more it costs than non-peak (%)."""
    result = df.groupby(["seasonality", "season_type"]).agg(
        booking_count=("id", "count"),
        avg_total_fare=("total_fare", "mean"),
    ).reset_index()

    non_peak = df[df["season_type"] == "Non-Peak"]
    if len(non_peak) > 0:
        non_peak_average = non_peak["total_fare"].mean()
        result["pct_vs_non_peak"] = (result["avg_total_fare"] - non_peak_average) / non_peak_average * 100
    else:
        result["pct_vs_non_peak"] = None  # nothing to compare with

    result["avg_total_fare"] = result["avg_total_fare"].round(2)
    result["pct_vs_non_peak"] = result["pct_vs_non_peak"].astype(float).round(2)
    return result.sort_values("avg_total_fare", ascending=False)


def peak_kpi(df):
    """KPI 2b: peak seasons together vs non-peak."""
    result = df.groupby("season_type").agg(
        booking_count=("id", "count"),
        avg_total_fare=("total_fare", "mean"),
    ).reset_index()
    result["avg_total_fare"] = result["avg_total_fare"].round(2)
    return result


def route_kpi(df):
    """KPI 4: the most frequent routes (source -> destination) by number of booking records."""
    result = df.groupby(["route", "source", "destination"]).agg(
        booking_count=("id", "count"),
        avg_total_fare=("total_fare", "mean"),
    ).reset_index()
    result = result.sort_values("booking_count", ascending=False).head(TOP_ROUTES)
    result["avg_total_fare"] = result["avg_total_fare"].round(2)
    result.insert(0, "route_rank", range(1, len(result) + 1))  # 1, 2, 3, ...
    return result


def calculate_kpis_dataframe(df):
    """Calculate all KPIs. Return them in a dictionary: table name -> result."""
    kpis = {
        "kpi_airline": airline_kpi(df),
        "kpi_season": season_kpi(df),
        "kpi_peak": peak_kpi(df),
        "kpi_route": route_kpi(df),
    }
    for name, result in kpis.items():
        print("\n" + name)
        print(result.to_string(index=False))
    return kpis


def calculate_kpis():
    """Read transformed_flights, calculate the KPIs and save each one in MySQL."""
    engine = get_mysql_engine()
    df = pd.read_sql("SELECT * FROM transformed_flights", engine)

    kpis = calculate_kpis_dataframe(df)

    for name, result in kpis.items():
        result.to_sql(name, engine, if_exists="replace", index=False)
    print("\nSaved", len(kpis), "KPI tables in MySQL")


if __name__ == "__main__":
    calculate_kpis()
