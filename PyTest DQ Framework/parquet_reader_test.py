"""
Test script to verify ParquetReader functionality.
"""

import os
import pandas as pd
from src.connectors.file_system.parquet_reader import ParquetReader


PARQUET_PATH = r"C:\Users\Vladyslav_Buzan\Documents\parquet_data\patient_sum_treatment_cost_per_facility_type"  # update this


def main():
    try:
        reader = ParquetReader()
        # Process Parquet files
        df = reader.process(PARQUET_PATH)

        # Basic checks
        print("ParquetReader loaded data successfully!")
        print(f"Number of rows: {len(df)}")
        print(f"Number of columns: {len(df.columns)}")
        print("Columns:", df.columns.tolist())
        print("First 5 rows:\n", df.head())

        # Example validation
        assert isinstance(df, pd.DataFrame), "Returned object is not a pandas DataFrame"
        assert len(df) > 0, "DataFrame is empty"
        assert all(col is not None for col in df.columns), "Some columns have None as name"

        print("All checks passed. ParquetReader works as expected!")

    except FileNotFoundError:
        print(f"Error: Path {PARQUET_PATH} does not exist")
    except AssertionError as ae:
        print(f"Assertion failed: {ae}")
    except Exception as e:
        print(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()
