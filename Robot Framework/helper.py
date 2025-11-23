import pandas as pd
import os
import time
from selenium.webdriver.common.by import By
import pyarrow.parquet as pq


# -----------------------------
# Selenium helpers
# -----------------------------
def safe_find_element(parent, by, value):
    """Return element or None if not found."""
    try:
        return parent.find_element(by, value)
    except:
        return None


# def read_custom_svg_table(table_element):
#     """
#     Extracts data from non-standard SVG table:
#     <g class="table">
#         <g class="y-column"> ... </g>
#         <g class="y-column"> ... </g>
#         <g class="y-column"> ... </g>
#     """
#
#
#     data = {}
#
#     # Get each column (<g class="y-column">)
#     columns = table_element.find_elements(By.CSS_SELECTOR, "g.y-column")
#
#     for col_index, col in enumerate(columns):
#
#         # --- HEADER ---
#         header_block = safe_find_element(col, By.ID, "header")
#         header_text = header_block.text.strip() if header_block else f"Column_{col_index + 1}"
#
#         col_values = []
#
#         # --- ALL BLOCKS (header + data blocks) ---
#         blocks = col.find_elements(By.CSS_SELECTOR, "g.column-block")
#
#         # FIX: sort blocks by ID (header, cells1, cells2)
#         blocks = sorted(blocks, key=lambda b: b.get_attribute("id") or "")
#
#         for block in blocks:
#             block_id = block.get_attribute("id")
#
#             # Skip header block, we already extracted text
#             if block_id == "header":
#                 continue
#
#             # Find all <text class="cell-text"> inside column
#             # This is the MOST reliable way in your HTML structure
#             cell_texts = block.find_elements(By.CSS_SELECTOR, "text.cell-text")
#
#             for t in cell_texts:
#                 col_values.append(t.text.strip())
#
#         # Save column
#         data[header_text] = col_values
#
#     # Align different length columns
#     max_len = max(len(v) for v in data.values())
#     for k, v in data.items():
#         if len(v) < max_len:
#             data[k] = v + [""] * (max_len - len(v))
#
#     df = pd.DataFrame(data)
#     return df

def read_custom_svg_table(table_element):
    script = """
        const table = arguments[0];
        const yColumns = table.querySelectorAll('g.y-column');
        if (yColumns.length < 3) return null;

        const headers = [];
        const col1 = [], col2 = [], col3 = [];

        // Process each of the 3 columns
        for (let i = 0; i < 3; i++) {
            const col = yColumns[i];
            let headerText = null;

            // Find header block (id="header") and extract text
            const headerBlock = col.querySelector('g.column-block#header text.cell-text');
            if (headerBlock) {
                // Clean up <br> → space or newline
                headerText = headerBlock.textContent
                    .trim()
                    .replace(/\\s*<br>\\s*/gi, ' ')
                    .replace(/\\s+/g, ' ');
            }
            headers.push(headerText || `Column ${i + 1}`);

            // Extract data rows (skip header block)
            const dataBlocks = col.querySelectorAll('g.column-block');
            dataBlocks.forEach(block => {
                if (block.id === 'header') return;

                const texts = block.querySelectorAll('text.cell-text');
                texts.forEach(t => {
                    const txt = t.textContent.trim();
                    if (txt) {
                        if (i === 0) col1.push(txt);
                        if (i === 1) col2.push(txt);
                        if (i === 2) col3.push(txt);
                    }
                });
            });
        }

        // Align all columns to shortest length
        const minLen = Math.min(col1.length, col2.length, col3.length);

        return {
            headers: headers,
            data: [
                col1.slice(0, minLen),
                col2.slice(0, minLen),
                col3.slice(0, minLen)
            ]
        };
        """

    result = table_element.parent.execute_script(script, table_element)

    if not result:
        return pd.DataFrame(columns=['1', '2', '3'])

    # Build DataFrame with real headers
    df = pd.DataFrame({
        result['headers'][0]: result['data'][0],
        result['headers'][1]: result['data'][1],
        result['headers'][2]: result['data'][2],
    })

    return df


# -----------------------------
# DataFrame helpers
# -----------------------------
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def filter_dataframe_by_date(df: pd.DataFrame, date_column: str, filter_date: str) -> pd.DataFrame:
    if date_column not in df.columns:
        raise ValueError(f"Column '{date_column}' not found in DataFrame")
    return df[df[date_column] == filter_date].reset_index(drop=True)


def read_parquet_dataset(folder_path: str, filter_date: str = None) -> pd.DataFrame:
    if not os.path.exists(folder_path):
        raise FileNotFoundError(f"Parquet folder does not exist: {folder_path}")

    table = pq.read_table(folder_path)
    df = table.to_pandas(date_as_object=True)

    if filter_date:
        date_col = next((c for c in df.columns if "date" in c.lower()), None)
        if date_col:
            df = df[df[date_col] == filter_date]

    if 'partition_date' in df.columns:
        df = df.drop(columns=['partition_date'])

    return df.reset_index(drop=True)


def normalize_numeric_columns(df: pd.DataFrame, numeric_cols: list) -> pd.DataFrame:
    df = df.copy()
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].astype(float)
    return df


def map_columns_for_comparison(df_html: pd.DataFrame, df_parquet: pd.DataFrame):
    column_mapping = {
        "average time spent": "avg_time_spent",
        "facility type": "facility_type",
        "visit date": "visit_date"
    }

    df_html = df_html.rename(columns=lambda x: x.strip().lower())
    df_parquet = df_parquet.rename(columns=lambda x: x.strip().lower())

    df_html_mapped = df_html.rename(columns=lambda x: column_mapping.get(x, x))
    df_html_mapped = df_html_mapped[[c for c in df_html_mapped.columns if c in df_parquet.columns]]

    # Dates to string
    for col in df_html_mapped.columns:
        if 'date' in col:
            df_html_mapped[col] = df_html_mapped[col].astype(str)
            df_parquet[col] = df_parquet[col].astype(str)

    # Numeric
    for col in ['avg_time_spent']:
        if col in df_html_mapped.columns:
            df_html_mapped[col] = df_html_mapped[col].astype(float)
        if col in df_parquet.columns:
            df_parquet[col] = df_parquet[col].astype(float)

    sort_cols = [c for c in ['facility_type', 'visit_date', 'avg_time_spent'] if c in df_html_mapped.columns]
    df_html_mapped = df_html_mapped.sort_values(by=sort_cols).reset_index(drop=True)
    df_parquet_sorted = df_parquet.sort_values(by=sort_cols).reset_index(drop=True)

    return df_html_mapped, df_parquet_sorted


def compare_dataframes(df_html: pd.DataFrame, df_parquet: pd.DataFrame):
    df_html_mapped, df_parquet_sorted = map_columns_for_comparison(df_html, df_parquet)

    if df_html_mapped.equals(df_parquet_sorted):
        return True, pd.DataFrame()

    diff = pd.concat(
        [
            df_html_mapped.assign(_origin="HTML"),
            df_parquet_sorted.assign(_origin="PARQUET")
        ]
    ).drop_duplicates(keep=False)

    return False, diff


