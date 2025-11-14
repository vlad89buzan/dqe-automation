import pytest
import pandas as pd

# Fixture to read the CSV file
@pytest.fixture(scope="session")
def read_csv():
    """
    Fixture to read a CSV file and return its content as a pandas DataFrame.

    Usage example:
        def test_data(read_csv):
            df = read_csv("src/data/data.csv")
            assert not df.empty
    """

    def _read_csv(path_to_file):
        return pd.read_csv(path_to_file)

    return _read_csv

# Fixture to validate the schema of the file
@pytest.fixture(scope="session")
def validate_schema():
    """
    Fixture to validate the schema of a DataFrame.
    Compares actual DataFrame columns to an expected list of column names.

    Parameters:
        actual_schema: list or Index (DataFrame.columns)
        expected_schema: list of expected column names

    Example:
        def test_schema(read_csv, validate_schema):
            df = read_csv("src/data/data.csv")
            validate_schema(df.columns, ["name", "age", "country"])
    """

    def _validate_schema(actual_schema, expected_schema):
        missing_cols = [col for col in expected_schema if col not in actual_schema]
        unexpected_cols = [col for col in actual_schema if col not in expected_schema]

        assert not missing_cols, f"Missing columns: {missing_cols}"
        assert not unexpected_cols, f"Unexpected columns: {unexpected_cols}"

    return _validate_schema


# Pytest hook to mark unmarked tests with a custom mark
def pytest_collection_modifyitems(config, items):
    """
    Pytest hook that runs after test collection.
    It checks each test, and if it has no custom markers,
    adds a default 'unmarked' marker automatically.
    """
    for item in items:
        # Skip if the test already has any user-defined marks
        if item.own_markers:
            continue

        # Add your custom mark
        item.add_marker(pytest.mark.unmarked)