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
    CONCAT(p.first_name, ' ', p.last_name) AS full_name,
    SUM(v.treatment_cost) AS sum_treatment_cost
FROM
    visits v
JOIN facilities f
    ON f.id = v.facility_id
JOIN patients p
    ON p.id = v.patient_id
GROUP BY
    f.facility_type,
    full_name)
select *,facility_type AS facility_type_partition from cte

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
    subfolder = "patient_sum_treatment_cost_per_facility_type"
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
    columns_to_check = ["facility_type", "full_name", "sum_treatment_cost"]
    data_quality_library.check_not_null_values(source_data,column_names=columns_to_check)

@pytest.mark.source_to_target
def test_check_count(source_data, target_data, data_quality_library):
    data_quality_library.check_count(source_data, target_data)

@pytest.mark.source_to_target
def test_check_data_full_data_set(source_data, target_data, data_quality_library):
    data_quality_library.check_data_full_data_set(source_data, target_data)

@pytest.mark.validity
def test_check_column_validity(source_data, data_quality_library):
    data_quality_library.check_column_validity(
        df=source_data,
        column_rules={
            "sum_treatment_cost": {"min": 0},  # must be >= 0
        }
    )