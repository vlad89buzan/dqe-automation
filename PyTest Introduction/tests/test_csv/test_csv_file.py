import pytest
import re


def test_file_not_empty(read_csv):
    df = read_csv("src/data/data.csv")
    assert not df.empty, "CSV file should not be empty"


@pytest.mark.validate_csv
@pytest.mark.xfail
def test_duplicates(read_csv):
    df = read_csv("src/data/data.csv")
    duplicates = df[df.duplicated()]

    assert duplicates.empty, f"Duplicate rows found:\n{duplicates}"


@pytest.mark.validate_csv
def test_validate_schema(read_csv, validate_schema):
    df = read_csv("src/data/data.csv")
    validate_schema(df.columns, ["id", "name", "age", "email", "is_active"])


@pytest.mark.validate_csv
@pytest.mark.skip
def test_age_column_valid(read_csv):
    df = read_csv("src/data/data.csv")
    assert "age" in df.columns, "Column 'age' is missing from the CSV"
    invalid_ages = df[(df["age"] < 0) | (df["age"] > 100)]
    assert invalid_ages.empty, f"Found invalid ages: {invalid_ages['age'].tolist()}"


@pytest.mark.validate_csv
def test_email_column_valid(read_csv):
    df = read_csv("src/data/data.csv")

    # Make sure the email column exists
    assert "email" in df.columns, "The CSV file must contain an 'email' column."

    # Define a simple email regex pattern
    email_pattern = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")

    # Find invalid emails (including NaN)
    invalid_emails = df[
        df["email"].isnull() | ~df["email"].astype(str).str.match(email_pattern)
        ]

    assert invalid_emails.empty, (
        f"Invalid email addresses found: {', '.join(invalid_emails['email'].astype(str).tolist())}"
    )


@pytest.mark.parametrize(
    "id, is_active",
    [
        (1, False),
        (2, True),
    ]
)
def test_active_players(read_csv, id, is_active):
    df = read_csv("src/data/data.csv")

    # Make sure required columns exist
    assert "id" in df.columns and "is_active" in df.columns, \
        "The CSV file must contain 'id' and 'is_active' columns."

    # Filter by id
    player_row = df[df["id"] == id]

    # Validate the record exists
    assert not player_row.empty, f"No player found with id={id}"

    # Check the expected value
    actual_status = player_row.iloc[0]["is_active"]
    assert actual_status == is_active, (
        f"Player with id={id} expected is_active={is_active}, "
        f"but found {actual_status}"
    )


def test_active_player(read_csv):
    df = read_csv("src/data/data.csv")

    # Make sure required columns exist
    assert "id" in df.columns and "is_active" in df.columns, \
        "The CSV file must contain 'id' and 'is_active' columns."

    # Filter by id
    player_row = df[df["id"] == 2]

    # Validate the record exists
    assert not player_row.empty, "No player found with id=2"

    # Check expected value
    actual_status = player_row.iloc[0]["is_active"]
    expected_status = True

    assert actual_status == expected_status, (
        f"Player with id=2 expected is_active={expected_status}, "
        f"but found {actual_status}"
    )
