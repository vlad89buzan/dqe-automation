*** Settings ***
Library           SeleniumLibrary
Library           helper.py
Library           Collections

Suite Setup       Open Report In Browser
Suite Teardown    Close Browser

*** Variables ***
${REPORT_FILE}        file://${CURDIR}/report.html
${PARQUET_FOLDER}     ${CURDIR}/facility_type_avg_time_spent_per_visit_date
${FILTER_DATE}        2025-11-13
${HTML_DATE_COL}      visit date          # exact name AFTER normalization (lowercase)

*** Keywords ***
Open Report In Browser
    Open Browser    ${REPORT_FILE}    chrome
    Maximize Browser Window
    Wait Until Element Is Visible    css:g.table text.cell-text    10s

Extract And Filter HTML Table
    ${table}=    Get WebElement    css:g.table

    # 1. Parse raw SVG table
    ${df}=    Read Custom Svg Table    ${table}
    Log     HTML DataFrame full:\n${df.to_string()}


    # 2. Normalize column names BEFORE filtering!
    ${df}=    Normalize Columns    ${df}


    # 3. Filter by date
    ${df_filtered}=    Filter Dataframe By Date    ${df}    ${HTML_DATE_COL}    ${FILTER_DATE}
    Log    HTML DataFrame filtered for ${FILTER_DATE}:\n${df_filtered}

    Log    HTML rows for ${FILTER_DATE}: ${df_filtered.shape[0]}

    RETURN    ${df_filtered}

Load Parquet Data
    ${df_parquet}=    Read Parquet Dataset    ${PARQUET_FOLDER}    ${FILTER_DATE}
    Log    Parquet DataFrame filtered for ${FILTER_DATE}:\n${df_parquet}
    Log    Parquet rows for ${FILTER_DATE}: ${df_parquet.shape[0]}
    RETURN    ${df_parquet}

Compare HTML vs Parquet
    [Arguments]    ${df_html}    ${df_parquet}

    # Show DataFrames before comparison
    Log    ===== HTML DataFrame BEFORE comparison =====\n${df_html}
    Log    ===== PARQUET DataFrame BEFORE comparison =====\n${df_parquet}

    ${equal}    ${diff}=    Compare Dataframes    ${df_html}    ${df_parquet}

    Run Keyword If    not ${equal}
    ...    Fail    DATA MISMATCH on ${FILTER_DATE}!\n${diff}

    Log    DataFrames match perfectly for ${FILTER_DATE}!

*** Test Cases ***
Validate Facility Type Average Time Spent Report

    ${df_html}=    Extract And Filter HTML Table
    ${df_parquet}=    Load Parquet Data
    Compare HTML vs Parquet    ${df_html}    ${df_parquet}
