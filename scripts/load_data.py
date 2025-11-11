
# ================= Overview =====================
# This file performs the following tasks:
# 1. The Focus: using python, extract data from two public datasets and load them - both Postgres tables and CSV files.
# Note: minimal data transformation is performed here; the focus is on extraction and loading.
#  Transformations and data cleaning is limited to standardizing column names to 'snake_case' and converting string values to lowercase. 
# 2. Public Datasets used: OEWS by State and O*NET Skills & Occupation files.
# ================================================

# ========= Public Datasets' Description =========
# 1. OEWS by State: provides occupational employment and wage estimates for various states in the US. 
# It includes information on employment levels, wages, and other related data for different occupations.
# 2. O*NET Skills & Occupation files: contains detailed information about various occupations, including the skills required 
# for each occupation. It provides insights into the knowledge, skills, abilities, and other characteristics needed for different jobs.

# ======== Dependencies Installation ========
# Before running the script, ensure you have the required libraries installed.
# --- pip install sqlalchemy requests ydata-profiling matplotlib
# --- pip install pandas openpyxl psycopg2-binary beautifulsoup4
# ===========================================

# ========= Import Lbraries and Dependencies =========
from __future__ import annotations     # Enables postponed evaluation of type hints for cleaner, forward-compatible annotations

import io                              # handling of in-memory file-like objects; for reading HTML/text streams
import os                              # operating system; reading environment variables
import re                              # Regular expressions for text pattern matching and data cleaning
import pandas as pd                    # Python Data Analysis Library; for data manipulation and analysis
import requests                        # HTTP client for fetching web data (HTML tables, text files from URLs)
import zipfile                        # handling ZIP files; for extracting compressed datasets
from sqlalchemy import create_engine, text   # SQLAlchemy tool to create a database connection engine
from sqlalchemy.dialects.postgresql import BIGINT, NUMERIC, TEXT  # PostgreSQL-specific column types for precise table schema control
from typing import Dict, Callable, Optional      # Code clarity; type hints for dictionaries (e.g., Dict[str, str])



# ========= Data Extracttion from Public Datasets =========
# This section does the following:
#     1. Download the public datasets and convert them into pandas DataFrames.
#     2. Convert DataFrame column names to 'snake_case' naming convention.
#     3. Convert all string values in the DataFrames to lowercase for consistency.
# =========================================================   

# ----- Set variables for public URLs ------
oews_url = "https://www.bls.gov/oes/special-requests/oesm24st.zip"
o_net_skills_url = "https://www.onetcenter.org/dl_files/database/db_30_0_excel/Skills.xlsx"

# ************ Data Extraction ************
# ++++++++ OEWS by State Extraction ++++++++
# --- Download ZIP into memory
headers = {"User-Agent": "Mozilla/5.0"}  # pretend to be a browser
resp = requests.get(oews_url, headers=headers)
resp.raise_for_status()

# --- Open the ZIP and list its contents
with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
    print("Files in ZIP:", z.namelist())
    # Note: this should display ['oesm24st/state_M2024_dl.xlsx'] because it is downloading 2024 data

    # Extract the Excel file; the first sheet
    oews_excel_file = [f for f in z.namelist() if f.lower().endswith(".xlsx")][0]

    # Read it directly into pandas
    with z.open(oews_excel_file) as f:
        oews_df = pd.read_excel(f)

# ---- Verify OEWS DataFrame ----
# print(oews_df.shape)
# print(oews_df.head())
# print(oews_df.describe())
# print(oews_df.columns)
# print(len(oews_df.columns)) # number of columns
# print(len(oews_df))
# print(oews_df.info())


# ++++++++ O*NET Datasets Extraction ++++++++
# This function fetches O*NET data from given URL and return a DataFrame
def fetch_onet_data(url: str) -> pd.DataFrame:
    # Download file into memory (using a browser-like header)
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers, timeout=60)
    resp.raise_for_status()  # will raise HTTPError if download fails

    # Read Excel directly from memory into pandas
    df = pd.read_excel(io.BytesIO(resp.content))

    # # (Optional) show basic info
    # print(f"Loaded {len(df):,} rows and {len(df.columns)} columns.")
    return df

#  ------ Run function to get O_Net DataFrames ------
o_net_skills_df = fetch_onet_data(o_net_skills_url)

# ---- Verify O*NET DataFrames ----
# print(o_net_occupation_titles_df.head())
# # print(o_net_skills_df.shape)
# print(o_net_skills_df.columns)
# print(len(o_net_occupations_df.columns)) # number of columns
# print(len(o_net_skills_df))
# print(o_net_occupations_df.info())

# ************ Data Cleaning ************
# --- This function convert columns (to 'snake_case') and convert string values to lowercase ---
# It removes special characters, replaces spaces/hyphens/slashes with underscores, and trims edges.
# It can also rename known columns using a provided mapping dictionary. Specifically useful for O*Net data.
# It returns a DataFrame

def clean_extracted_dataframes(
    df: pd.DataFrame,
    rename_map: Optional[Dict[str, str]] = None,
    lowercase_values: bool = False
) -> pd.DataFrame:
    df = df.copy()

    # ---- Clean column names ----
    cleaned_cols = []
    for col in df.columns:
        new_col = col.lower()                        # lowercase
        new_col = re.sub(r"[\s\-/]+", "_", new_col)  # replace spaces/hyphens/slashes with underscores
        new_col = re.sub(r"[^a-z0-9_]", "", new_col) # remove non-alphanumeric
        new_col = re.sub(r"_+", "_", new_col)        # collapse multiple underscores
        new_col = new_col.strip("_")                 # trim leading/trailing underscores
        cleaned_cols.append(new_col)

    df.columns = cleaned_cols

    # ---- Apply rename map (e.g. for O*Net data) ----
    if rename_map:
        df.rename(columns=rename_map, inplace=True)

    # ---- convert all string values to lowercase  ----
    if lowercase_values:
        df = df.applymap(lambda x: x.lower() if isinstance(x, str) else x)

    return df


o_net_skills_format = pd.DataFrame({
    "O*NET-SOC Code": ["11-1011.00", "13-2011.00"],
    "Title": ["Chief Executives", "Accountants"],
    "Element Name": ["Reading Comprehension", "Active Listening"],
    "Scale ID": ["LV", "IM"]
})

o_net_skills_rename_map = {
    "onet_soc_code": "soc_code",
    "title": "occupation_title",
    "element_name": "skill_name"
}

# -------- Apply function to Dataframe to clean them --------
dataframes = {
    "oews_raw_df": oews_df,
    "onet_skills_raw_df": o_net_skills_df
}

cleaned_dfs = {}

for name, df in dataframes.items():
    print(f"Cleaning {name}...")
    cleaned_df = clean_extracted_dataframes(df, lowercase_values=True)
    cleaned_df.name = name # set the name attribute
    cleaned_dfs[name] = cleaned_df

# print("✅ Cleaning complete!")
# print(cleaned_dfs.keys())

# Test & view results
# for name, df in cleaned_dfs.items():
#     print(f"{name}: {df.shape}, columns: {df.columns[:5].tolist()}")


# ************ Data Loading Section ************
# There are two fuunctions in this section:
# 1. Saves the output of cleaned DF as a csv file. Can be easily called for pandas analyssis later.
# 2. The second function loads the DF into PostreSQL tables


# +++++++ 1. Save cleaned DF as csv file +++++++
output_dir = "data_output/raw"  # output directory to save raw/cleaned parquet/csv files

def save_dataframes_as_csv(dataframes: dict, output_dir: str) -> None:
    
    # Save multiple DataFrames as CSV.

    # Args:
    #     dataframes (dict): A dictionary where keys are file names (without extension) 
    #                        and values are pandas DataFrames.
    #     output_dir (str): The directory where the Parquet/csv files will be saved.
    
    for name, df in cleaned_dfs.items():
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
save_dataframes_as_csv(cleaned_dfs, output_dir)


# --------- Perform some Tests on saved files ---------
# --- Testing on saved DF files ----
full_path = os.path.join(output_dir, 'oews_raw_df.csv') # join folder path and file together for a single path to use in pd.readcsv()
filtered_df = pd.read_csv(full_path)
# print(filtered_df.head())
# print(filtered_df.columns)
# print(filtered_df.describe())

# --- Test for filtered data, e.g. MD Data ---
# md_filtered_df = filtered_df[filtered_df['prim_state'] == 'md']
# print(md_filtered_df.head(10))
# print(md_filtered_df.info())
# --------- End of Testing ---------

# +++++++ 2. Load a DataFrame into a Postgres table +++++++
def load_df_to_postgres(
    df: pd.DataFrame,
    table_name: str,
    pg_uri: str,
    schema: str = "public",
    if_exists: str = "replace",
    chunksize: int = 1000
) -> None:
    
    # Validate DataFrame; ensure not empty or not None. Otherwise, raise error
    if df is None or df.empty:
        raise ValueError(f"DataFrame for table '{table_name}' is empty. Nothing to load.")

    # 1) Create connection to Postres DB using SQLAlchemy engine. Future = True (compatible with newer features.)
    engine = create_engine(pg_uri, future=True)

    # 2) Ensure schema exists & can connect; adapted to create if not exists.
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))

    # 3) Write the DataFrame to Postgres table
    #    - index=False: don’t create an extra "index" column
    #    - to_sql will auto-create the table with inferred column types
    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists=if_exists,
        index=False,
        chunksize=chunksize,
        method="multi",   # faster batch inserts
    )

    # print(f"✅ Loaded {len(df):,} rows into {schema}.{table_name}")
    
    # Create placeholder for index on table
# with engine.connect() as conn:
#     conn.execute("CREATE INDEX idx_column_name ON table_name(column_name);")    

# +++++++ Call the function to load DataFrames into Postgres +++++++
# --- Get Postgres connection URI from environment variable ---
PG_URI = os.getenv("PG_URI")
if not PG_URI:
    raise ValueError("Environment variable 'PG_URI' is not set.")

# --- Load multiple DataFrames into Postgres tables in a loop ---
schema = "raw"  # specify target schema

for name, df in cleaned_dfs.items():
    table_name = name.replace("_df", "")  # e.g., "oews_raw_df" -> "oews_raw"
    load_df_to_postgres(df, table_name=table_name, pg_uri=PG_URI, schema=schema, if_exists="replace")

