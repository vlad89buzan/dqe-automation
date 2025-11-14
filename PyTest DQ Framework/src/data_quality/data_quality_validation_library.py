import pandas as pd


class DataQualityLibrary:
    """
    A library of static methods for performing data quality checks on pandas DataFrames.

    This class is intended to be used in a PyTest-based testing framework to validate
    the quality of data in DataFrames. Each method performs a specific data quality
    check and uses assertions to ensure that the data meets the expected conditions.
    """

    @staticmethod
    def check_duplicates(df: pd.DataFrame, column_names=None):
        """
        Check for duplicates in the DataFrame.

        Args:
            df (pd.DataFrame): DataFrame to check.
            column_names (list, optional): Columns to check duplicates on.
                If None, checks entire row.

        Raises:
            AssertionError: If duplicates are found, showing counts.
        """
        if column_names:
            # Count duplicates based only on the subset of columns
            duplicates = df[df.duplicated(subset=column_names, keep=False)]
            if not duplicates.empty:
                # Show counts of duplicates for clarity
                dup_counts = (
                    duplicates.groupby(column_names)
                    .size()
                    .reset_index(name='count')
                    .sort_values('count', ascending=False)
                )
                raise AssertionError(
                    f"Duplicate rows found on columns {column_names}:\n{dup_counts}"
                )
        else:
            # Full-row duplicates
            duplicates = df[df.duplicated(keep=False)]
            if not duplicates.empty:
                # Count duplicates based on all columns
                dup_counts = (
                    duplicates.groupby(list(df.columns))
                    .size()
                    .reset_index(name='count')
                    .sort_values('count', ascending=False)
                )
                raise AssertionError(
                    f"Duplicate full rows found:\n{dup_counts}"
                )

    @staticmethod
    def check_count(df1: pd.DataFrame, df2: pd.DataFrame):
        """Check that two DataFrames have the same number of rows."""
        assert len(df1) == len(df2), f"Row count mismatch: {len(df1)} != {len(df2)}"

    @staticmethod
    def check_data_full_data_set(df1, df2,subset_columns=None):
        """
        Check that two datasets match exactly, like UNION ALL of EXCEPT in SQL.
        Automatically aligns column types for comparison (e.g., datetime vs object).
        Shows which rows are in one dataset but not the other, with counts.

        Args:
            df1 (pd.DataFrame): First dataset (source/expected).
            df2 (pd.DataFrame): Second dataset (target/actual).
            subset_columns (list, optional): Columns to compare. If None, compare all columns.

        Raises:
            AssertionError: If any row mismatches exist.
        """
        # Columns to compare
        columns = subset_columns or df1.columns.tolist()

        # Align column types
        for col in columns:
            if col not in df2.columns:
                raise ValueError(f"Column '{col}' not found in df2")
            # If either column is datetime, convert both to datetime
            if pd.api.types.is_datetime64_any_dtype(df1[col]) or pd.api.types.is_datetime64_any_dtype(df2[col]):
                df1[col] = pd.to_datetime(df1[col], errors='coerce')
                df2[col] = pd.to_datetime(df2[col], errors='coerce')
            # If either column is numeric but different type, convert both to float
            elif pd.api.types.is_numeric_dtype(df1[col]) or pd.api.types.is_numeric_dtype(df2[col]):
                df1[col] = pd.to_numeric(df1[col], errors='coerce')
                df2[col] = pd.to_numeric(df2[col], errors='coerce')
            # Otherwise, compare as string
            else:
                df1[col] = df1[col].astype(str)
                df2[col] = df2[col].astype(str)

        # Rows in df1 but not in df2
        diff1 = df1.merge(df2, on=columns, how='left', indicator=True).query('_merge == "left_only"')[columns].copy()
        diff1['diff_type'] = 'in source not in target'

        # Rows in df2 but not in df1
        diff2 = df2.merge(df1, on=columns, how='left', indicator=True).query('_merge == "left_only"')[columns].copy()
        diff2['diff_type'] = 'in target not in source'

        # Combine differences
        differences = pd.concat([diff1, diff2], ignore_index=True)

        if not differences.empty:
            # Count duplicates for clarity
            diff_counts = differences.groupby(columns + ['diff_type']).size().reset_index(name='count')
            raise AssertionError(
                f"Datasets do not match! Differences found:\n{diff_counts.to_string(index=False)}"
            )

    @staticmethod
    def check_dataset_is_not_empty(df: pd.DataFrame):
        """Check that the DataFrame is not empty."""
        assert not df.empty, "DataFrame is empty"

    @staticmethod
    def check_not_null_values(df: pd.DataFrame, column_names=None):
        """Check that specified columns do not contain null values."""
        if column_names:
            for col in column_names:
                assert df[col].notnull().all(), f"Null values found in column: {col}"
        else:
            assert df.notnull().all().all(), "Null values found in DataFrame"

    @staticmethod
    def check_column_validity(
            df: pd.DataFrame,
            column_rules: dict
    ) -> pd.DataFrame:
        """
        Universal column validity checker.

        This method validates columns in a DataFrame according to user-defined rules.
        It supports numeric range checks, membership checks, and custom conditions.

        Args:
            df (pd.DataFrame): DataFrame to check.
            column_rules (dict): Dictionary where keys are column names and values are rule dictionaries.
                Supported rule keys:
                    - "min": minimum allowed value (inclusive)
                    - "max": maximum allowed value (inclusive)
                    - "allowed_values": list of allowed values
                    - "condition": lambda function returning True for valid rows

        Returns:
            pd.DataFrame: DataFrame of invalid rows (empty if all rows are valid).

        Raises:
            AssertionError: If any invalid values are found.

        Example:
            ```python
            # Example 1: Check numeric ranges
            DataQualityLibrary.check_column_validity(
                df=source_data,
                column_rules={
                    "min_time_spent": {"min": 0, "max": 1000},
                    "duration_minutes": {"min": 1}
                }
            )

            # Example 2: Check allowed categorical values
            DataQualityLibrary.check_column_validity(
                df=source_data,
                column_rules={
                    "facility_type": {"allowed_values": ["Clinic", "Hospital", "Lab"]}
                }
            )

            # Example 3: Custom logic (dates must not be in the future)
            DataQualityLibrary.check_column_validity(
                df=source_data,
                column_rules={
                    "visit_date": {"condition": lambda x: x <= pd.Timestamp.today()}
                }
            )
            ```
        """
        all_invalid_rows = []

        for column, rules in column_rules.items():
            invalid_mask = pd.Series(False, index=df.index)

            # --- Numeric range checks ---
            if "min" in rules:
                invalid_mask |= df[column] < rules["min"]
            if "max" in rules:
                invalid_mask |= df[column] > rules["max"]

            # --- Allowed values check ---
            if "allowed_values" in rules:
                invalid_mask |= ~df[column].isin(rules["allowed_values"])

            # --- Custom condition check ---
            if "condition" in rules:
                invalid_mask |= ~df[column].apply(rules["condition"])

            # Collect invalid rows
            invalid_rows = df.loc[invalid_mask, [column]]
            if not invalid_rows.empty:
                invalid_rows = invalid_rows.assign(invalid_column=column)
                all_invalid_rows.append(invalid_rows)

        # Combine invalid rows across all columns
        if all_invalid_rows:
            invalid_df = pd.concat(all_invalid_rows)
            raise AssertionError(
                f"Invalid values found in the following columns:\n{invalid_df.head(20)}\n"
                f"(Total {len(invalid_df)} invalid rows)"
            )
        else:
            return pd.DataFrame(columns=df.columns)


