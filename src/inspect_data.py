"""First look at the CSV before building the pipeline (run once, by hand)."""
from pathlib import Path

import pandas as pd

CSV_FILE = Path(__file__).resolve().parents[1] / "data" / "Flight_Price_Dataset_of_Bangladesh.csv"


def inspect_data():
    """Print the size, column names, types, missing values and a few rows of the CSV."""
    df = pd.read_csv(CSV_FILE)

    print("Rows and columns:", df.shape)
    print("\nColumn names:")
    for column in df.columns:
        print(" -", column)

    print("\nData types:")
    print(df.dtypes)

    print("\nMissing values per column:")
    print(df.isnull().sum())

    print("\nFirst 5 rows:")
    print(df.head())

    if "Seasonality" in df.columns:
        print("\nSeasons in the data:")
        print(df["Seasonality"].value_counts())


if __name__ == "__main__":
    inspect_data()
