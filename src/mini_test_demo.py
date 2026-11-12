"""Mini test: run my real validation and transformation code on 11 small, made-up flights.

Each row in data/mini_test_flights.csv was written on purpose to test one situation.
Nothing is saved in the databases - this only prints the results.

Run it inside the Airflow container:
    docker compose exec airflow python -m src.mini_test_demo
"""
from pathlib import Path

from src.ingest_data import read_csv
from src.validate_data import validate_dataframe
from src.transform_data import transform_dataframe

MINI_CSV = Path(__file__).resolve().parents[1] / "data" / "mini_test_flights.csv"

# What I expect for each test row (row number -> expected result)
EXPECTED = {
    1: "valid",
    2: "valid (Eid = peak season)",
    3: "valid (peak season)",
    4: "rejected: negative base fare",
    5: "valid: total fare missing, filled by base + tax",
    6: "rejected: duplicate of row 2",
    7: "valid but flagged: total fare does not match base + tax",
    8: "rejected: missing airline",
    9: "rejected: 'Dhaka' is not a 3-letter airport code",
    10: "rejected: source and destination are the same",
    11: "valid, but season 'Ramadan' is unknown: season_type stays empty (not Peak)",
}


def run_mini_test():
    print("=" * 60)
    print("STEP 1 - Read the mini test file (same code as ingestion)")
    print("=" * 60)
    df = read_csv(MINI_CSV)
    df.insert(0, "id", range(1, len(df) + 1))  # row numbers 1, 2, 3, ...

    print()
    print("=" * 60)
    print("STEP 2 - Validation (same code as the validate_data task)")
    print("=" * 60)
    valid, rejected = validate_dataframe(df)

    print()
    print("REJECTED ROWS:")
    for index, row in rejected.iterrows():
        print("  Row", row["id"], "-", row["airline"], "->", row["problem"])

    print()
    print("=" * 60)
    print("STEP 3 - Transformation (same code as the transform_data task)")
    print("=" * 60)
    transformed = transform_dataframe(valid.copy())

    print()
    print("VALID ROWS AFTER TRANSFORMATION:")
    for index, row in transformed.iterrows():
        line = ("  Row " + str(row["id"]) + " - " + str(row["airline"])
                + " | total fare: " + str(row["total_fare"])
                + " | base + tax: " + str(row["calculated_total_fare"])
                + " | mismatch: " + str(row["fare_mismatch"])
                + " | " + str(row["season_type"]))
        print(line)

    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print("Rows in the test file:", len(df))
    print("Valid rows:           ", len(valid))
    print("Rejected rows:        ", len(rejected))
    print("Fare mismatches:      ", int(transformed["fare_mismatch"].sum()))

    print()
    print("WHAT I EXPECTED:")
    for row_number, expected in EXPECTED.items():
        print("  Row", row_number, "-", expected)


if __name__ == "__main__":
    run_mini_test()
