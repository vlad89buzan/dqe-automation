"""
Description: Data Quality checks ...
Requirement(s): TICKET-1234
Author(s): Name Surname
"""

import pytest
import os

@pytest.fixture(scope='module')
def target_data(db_connection):
    target_query = """
    with cte as (SELECT
    f.facility_type,
    v.visit_timestamp::date AS visit_date,
    ROUND(AVG(v.duration_minutes), 2) AS avg_time_spent
FROM
    visits v
JOIN
    facilities f 
    ON f.id = v.facility_id
GROUP BY
    f.facility_type,
    visit_date)
select *,TO_CHAR(visit_date, 'YYYY-MM') AS partition_date from cte

    """
    target_data = db_connection.get_data_sql(target_query)
    return target_data

@pytest.fixture(scope='module')
def source_data(parquet_reader):
    root_path = os.getenv(
        "PARQUET_ROOT_PATH",
        r"C:\Users\Vladyslav_Buzan\Documents\parquet_data"  # local default
    )

    # Subfolder specific to this check
    subfolder = "facility_type_avg_time_spent_per_visit_date"
    source_path = os.path.join(root_path, subfolder)
    source_data = parquet_reader.process(source_path)
    return source_data

@pytest.mark.smoke
def test_check_dataset_is_not_empty(source_data, data_quality_library):
    data_quality_library.check_dataset_is_not_empty(source_data)

@pytest.mark.smoke
def test_check_duplicates(source_data, data_quality_library):
    data_quality_library.check_duplicates(source_data)

@pytest.mark.smoke
def test_check_not_null_values(source_data, data_quality_library):
    columns_to_check = ["facility_type", "visit_date", "avg_time_spent"]
    data_quality_library.check_not_null_values(source_data,column_names=columns_to_check)

@pytest.mark.source_to_target
def test_check_count(source_data, target_data, data_quality_library):
    data_quality_library.check_count(source_data, target_data)

@pytest.mark.source_to_target
def test_check_data_full_data_set(source_data, target_data, data_quality_library):
    data_quality_library.check_data_full_data_set(source_data, target_data)