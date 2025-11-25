from selenium import webdriver
from selenium.webdriver.common.by import By
from helper import (
    read_custom_svg_table,
    normalize_columns,
    filter_dataframe_by_date,
    read_parquet_dataset,
    compare_dataframes

)
import os
import time

# -----------------------------
# Paths and parameters
# -----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
report_path = os.path.join(BASE_DIR, "report.html")
parquet_folder = os.path.join(BASE_DIR, "facility_type_avg_time_spent_per_visit_date")
filter_date = "2025-11-13"

# Column name in HTML table that contains the date
html_date_column = "visit date"

# -----------------------------
# Open browser and load HTML report
# -----------------------------
driver = webdriver.Chrome()
driver.get(f"file:///{report_path}")
time.sleep(1)  # wait for table to render

# Locate the table
table = driver.find_element(By.CSS_SELECTOR, "g.table")

# -----------------------------
# Parse HTML table
# -----------------------------
df_html = read_custom_svg_table(table)
print(df_html)
df_html = normalize_columns(df_html)  # lowercase & strip column names



# Filter HTML DataFrame by date
df_html_filtered = filter_dataframe_by_date(df_html, html_date_column, filter_date)
print("\n===== HTML TABLE DATAFRAME (Filtered) =====")
print(df_html_filtered)

# Close browser
driver.quit()

# -----------------------------
# Load Parquet dataset
# -----------------------------
df_parquet = read_parquet_dataset(parquet_folder, filter_date)
print("\n===== PARQUET DATAFRAME (Filtered) =====")
print(df_parquet)

# -----------------------------
# Compare HTML and Parquet DataFrames
# -----------------------------
is_equal, diff = compare_dataframes(df_html_filtered, df_parquet)
if is_equal:
    print("\n✅ DataFrames match successfully!")
else:
    print("\n❌ Data mismatch found:")
    print(diff)
