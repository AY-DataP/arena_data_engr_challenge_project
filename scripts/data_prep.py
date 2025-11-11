import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import re
from typing import Optional, Dict, List
from sqlalchemy import create_engine
from load_data import load_df_to_postgres # import the load function from load_data.py


# -----Get dataframes from raw data files-----
oews_df = pd.read_csv('data_output/raw/oews_raw_df.csv') 
onet_skills_df = pd.read_csv('data_output/raw/onet_skills_raw_df.csv')

# ---- Adhoc: test dataframees and columns ----
# print(oews_df.info())
# hourly_check = oews_df['hourly'].value_counts()
# annual_check = oews_df['annual'].value_counts()
# hourly_check = oews_df['hourly']
# annual_check = oews_df['annual']
# print(annual_check.head(10))
# print(hourly_check.head(10))

# ----- fields selection: filtering large datasets. Create a list of each df to filter -----
# ---------- OEWS fields list --------
oews_selected_fields = [
    'occ_code', 'occ_title', 'prim_state', 'tot_emp', 'jobs_1000', 'mean_prse', 'emp_prse'
    ,  'a_mean', 'a_median', 'a_pct10', 'a_pct25', 'a_pct75', 'a_pct90'
    , 'h_mean', 'h_median', 'h_pct10', 'h_pct25', 'h_pct75', 'h_pct90'
    , 'annual', 'hourly', 'pct_total', 'pct_rpt'
]

# ----- this function creates a new df based on selected fields. -----
# ----- it takes in two arguments: file path and list of fields to select -----
def dataframe_fields_selection(file_path: str, selected_fields: list) -> pd.DataFrame:
    """
    Read OEWS dataframe from CSV and select specified fields.

    Args:
        file_path (str): The path to the OEWS CSV file.
        selected_fields (list): A list of column names to select.

    Returns:
        pd.DataFrame: The dataframe with selected fields.
    """
    # Read the OEWS dataframe
    oews_df = pd.read_csv(file_path)

    # Create new dataframe with selected fields
    oews_selected_df = oews_df[selected_fields]

    return oews_selected_df

# ----- call the function to create a new dataframe with selected fields -----
oews_selected_df = dataframe_fields_selection('data_output/raw/oews_raw_df.csv', oews_selected_fields) 

# ----- rename dataframes to be cleaned -----
oews_selected_df = oews_selected_df.copy()
onet_skills_selected_df = onet_skills_df.copy()

# print(oews_selected_df.info()) #describe, head, info, shape, tail


# ----------- function to clean dataframe -----
# def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Clean a pandas DataFrame:
#     1. Trim spaces in string fields.
#     2. Replace non-numeric values with NULL (NaN) in numeric fields.
#     3. Convert data type to numeric for aggregation where applicable.

#     Args:
#         df (pd.DataFrame): The input DataFrame to clean.

#     Returns:
#         pd.DataFrame: The cleaned DataFrame.
#     """
#     df = df.copy()  # Create a copy of the DataFrame to avoid modifying the original

#     for column in df.columns:
#         # 1. Trim spaces in string fields
#         if df[column].dtype == 'object':  # Check if the column is a string
#             df[column] = df[column].apply(lambda x: x.strip() if isinstance(x, str) else x)

#         # 2. Replace non-numeric values with NULL (NaN) in numeric fields
#         if df[column].dtype in ['float64', 'int64', 'object']:  # Check if numeric or convertible
#             df[column] = pd.to_numeric(df[column], errors='coerce')   
#     return df

# ------- for some dataframes we may want to standadize selected fields; e.g. convert columns with numeric entries to proper column data type -----
oews_standardize_fields = [
    'tot_emp', 'jobs_1000', 'mean_prse', 'emp_prse'
    ,  'a_mean', 'a_median', 'a_pct10', 'a_pct25', 'a_pct75', 'a_pct90'
    , 'h_mean', 'h_median', 'h_pct10', 'h_pct25', 'h_pct75', 'h_pct90'
]

# ----- clean and convert numeric fields function -----
# this function cleans the dataframe and converts selected fields to numeric
def clean_dataframe(
    df: pd.DataFrame,
    numeric_fields: Optional[List] = None
) -> pd.DataFrame:
    """
    Clean a pandas DataFrame by:
      1. Trimming whitespace from string fields.
      2. Replacing empty strings with NaN.
      3. Optionally converting selected fields to numeric.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame to clean.
    numeric_fields : list of str, optional
        A list of column names to convert to numeric.
        If None, only trimming and NaN replacement are applied.

    Returns
    -------
    pd.DataFrame
        A cleaned DataFrame with standardized values and optional numeric conversion.
    """
    df = df.copy()  # avoid modifying the original DataFrame

    # --- Step 1: Trim whitespaces and replace blanks in ALL string/object columns ---
    for col in df.columns:
        if df[col].dtype == "object" or pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].astype(str).str.strip()  # trim whitespace
            df[col] = df[col].replace({"": pd.NA})     # replace empty strings with NaN

    # --- Step 2: Convert selected columns to numeric (if list provided) ---
    if numeric_fields:
        for col in numeric_fields:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            else:
                print(f"⚠️ Warning: Column '{col}' not found in DataFrame. Skipping.")

    return df
        


# --------- rename cleaned df; use function ----------
oews_cleaned_df = clean_dataframe(oews_selected_df, oews_standardize_fields) # apply the cleaning function to OEWS df
# onet_skills_cleaned_df = onet_skills_selected_df.copy() # this does not need to be cleaned further.
onet_skills_cleaned_df = clean_dataframe(onet_skills_selected_df) # apply the cleaning function to ONET Skills df; without selected fields for numeric conversion
# print(oews_cleaned_df.info())
# print(onet_skills_cleaned_df.info())

# ========= Load cleaned DataFrames into Postgres =========
# ---- this section loads multiple dataframes into Postgres ---
# =========================================================

# Create a dictionary to hold cleaned DataFrames
cleaned_dataframes: Dict[str, pd.DataFrame] = {
    "oews_cleaned": oews_cleaned_df,
    "onet_skills_cleaned": onet_skills_cleaned_df
}

#  -------- Call load_to_postgres function to load DataFrames into Postgres --------
# ---- note: load_to_postgres function from load_data.py already imported above ----

# 1) Get Postgres connection URI from environment variable
PG_URI = os.getenv("PG_URI")
if not PG_URI:
    raise ValueError("Environment variable 'PG_URI' is not set.")

# --------- Load multiple DataFrames into Postgres tables in a loop ----------
# ---specify schema path
schema = "curated"  # specify target schema

# ---- loop through the cleaned_dataframes dictionary and load each DataFrame ----
for name, df in cleaned_dataframes.items():
    table_name = name
    load_df_to_postgres(df, table_name=table_name, pg_uri=PG_URI, schema=schema, if_exists="replace")

# ----- Save cleaned DataFrames as CSV files ----
output_dir = "data_output/curated"
# save_dataframes_as_csv(cleaned_dataframes, output_dir)

def save_dataframes_as_csv(dataframes: dict, output_dir: str) -> None:
    
    # Save multiple DataFrames as CSV.

    # Args:
    #     dataframes (dict): A dictionary where keys are file names (without extension) 
    #                        and values are pandas DataFrames.
    #     output_dir (str): The directory where the Parquet/csv files will be saved.
    
    for name, df in cleaned_dataframes.items():
        # ensure df is not empty
        if df.empty:
            print(f"⚠️ Warning: DataFrame '{name}' is empty. Skipping save.")
            continue
        # construct file path   
        # file_path = f"{output_dir}/{name}.parquet"
        file_path = f"{output_dir}/{name}.csv"
        
        #  Save DataFrame as Parquet / csv file; and print confirmation. 
        # df.to_parquet(file_path, index=False)
        df.to_csv(file_path, index=False)
        # print(f"✅ Saved {name} to {file_path}")
        
    # print(f"\n🎉 All DataFrames saved successfully to: {output_dir}")    

# --- Call the function to save cleaned DataFrames as Parquet/csv files ----
save_dataframes_as_csv(cleaned_dataframes, output_dir)