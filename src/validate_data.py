"""Task 2 - Validation and cleaning: check the staging data and separate good rows from bad rows."""
# FLOW: Step 2 of 5 | ELTL: T (Transform, part 1 - validation)
# Before me: ingest_data.py filled raw_flights | After me: transform_data.py reads validated_flights

import pandas as pd

from src.db import get_mysql_engine

# The 6 columns the lab requires (lab name -> my column name)
REQUIRED_COLUMNS = {
    "Airline": "airline",
    "Source": "source",
    "Destination": "destination",
    "Base Fare": "base_fare",
    "Tax & Surcharge": "tax_surcharge",
    "Total Fare": "total_fare",
}

FARE_COLUMNS = ["base_fare", "tax_surcharge", "total_fare"]
TEXT_COLUMNS = ["airline", "source", "source_name", "destination", "destination_name",
                "stopovers", "aircraft_type", "travel_class", "booking_source", "seasonality"]


def check_required_columns(df):
    """Stop the pipeline if one of the required columns does not exist."""
    missing = []
    for lab_name, column in REQUIRED_COLUMNS.items():
        if column not in df.columns:
            missing.append(lab_name)
    if missing:
        raise ValueError("Required columns are missing: " + str(missing))
    print("All 6 required columns are present")


def unify_spelling(values):
    """Case-insensitive: 'biman', 'BIMAN' and 'Biman' all become the most common spelling."""
    # value_counts() lists the most common spelling first
    best_spelling = {}
    for name in values.value_counts().index:
        key = name.lower()
        if key not in best_spelling:
            best_spelling[key] = name

    result = []
    for value in values:
        if pd.isnull(value):
            result.append(value)
        else:
            result.append(best_spelling[value.lower()])
    return result


def clean_text(df):
    """Remove extra spaces, turn empty text into NULL, and make spellings consistent."""
    for column in TEXT_COLUMNS:
        df[column] = df[column].astype("string").str.strip()
        df[column] = df[column].replace("", pd.NA)

    # Airport codes are always capital letters: " dac" -> "DAC"
    df["source"] = df["source"].str.upper()
    df["destination"] = df["destination"].str.upper()

    # Same name written in different case -> one spelling
    df["airline"] = unify_spelling(df["airline"])
    df["seasonality"] = unify_spelling(df["seasonality"])
    return df


def add_problem(df, mask, problem):
    """Write a problem next to every row where mask is True, and print how many rows have it."""
    df.loc[mask, "problem"] = df.loc[mask, "problem"] + problem + "; "
    print(" -", problem + ":", int(mask.sum()), "rows")


def find_problems(df):
    """Check every row and write the reason in the 'problem' column (empty = good row)."""
    df["problem"] = ""
    print("Checking rows:")

    # Empty text in the categorical columns
    add_problem(df, df["airline"].isnull(), "missing airline")
    add_problem(df, df["source"].isnull(), "missing source")
    add_problem(df, df["destination"].isnull(), "missing destination")

    # Invalid airport codes: a valid code has exactly 3 letters (DAC, CXB, ...)
    for column in ["source", "destination"]:
        has_value = df[column].notnull()
        is_code = (df[column].str.len() == 3) & (df[column].str.isalpha())
        add_problem(df, has_value & ~is_code.fillna(False), "invalid " + column + " code")

    add_problem(df, df["source"] == df["destination"], "source equals destination")

    # Base fare and tax must be numbers (text was turned into NULL during ingestion).
    # A missing TOTAL fare is not a problem here: transform_data.py calculates it.
    add_problem(df, df["base_fare"].isnull(), "missing or non-numeric base fare")
    add_problem(df, df["tax_surcharge"].isnull(), "missing or non-numeric tax")

    # Negative fares are impossible
    for column in FARE_COLUMNS:
        add_problem(df, df[column] < 0, "negative " + column)

    # The same flight record twice: keep the first one
    columns_without_id = [column for column in df.columns if column not in ["id", "problem"]]
    add_problem(df, df.duplicated(subset=columns_without_id), "duplicate row")

    return df


def validate_dataframe(df):
    """Run all checks. Return (good rows, rejected rows with their problem)."""
    check_required_columns(df)
    df = clean_text(df)
    df = find_problems(df)

    df["problem"] = df["problem"].str.rstrip("; ")
    valid = df[df["problem"] == ""].drop(columns=["problem"])
    rejected = df[df["problem"] != ""]
    return valid, rejected


def validate_data():
    """Read raw_flights, validate it, and save validated_flights and rejected_flights in MySQL."""
    engine = get_mysql_engine()
    # INPUT: raw_flights (made in Step 1)
    df = pd.read_sql("SELECT * FROM raw_flights", engine)

    valid, rejected = validate_dataframe(df)

    # OUTPUT: good rows -> validated_flights (used in Step 3), bad rows -> rejected_flights (with the reason)
    valid.to_sql("validated_flights", engine, if_exists="replace", index=False, chunksize=1000)
    rejected.to_sql("rejected_flights", engine, if_exists="replace", index=False, chunksize=1000)

    print("Rows checked:", len(df), "| valid:", len(valid), "| rejected:", len(rejected))
    if len(valid) == 0:
        raise ValueError("No valid rows left - check rejected_flights in MySQL")


if __name__ == "__main__":
    validate_data()
