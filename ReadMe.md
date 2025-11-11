# Arena Challenge Demo

Welcome to the **Arena Challenge Demo** project! This repository contains the code and resources for demonstrating the Arena Challenge exercise.

Project: Public Data Extraction, Transformation & Loading (ETL)
Technologies: Python, Pandas, PostgreSQL, SQLAlchemy, Requests

## *** IMPORTANT NOTES: ***
# 1. docker - for Postgres - was not spin up. However, an existing local Postgres DB is used.
# 2. due to size limitations, CSV & XLSX files were not added as part of push to GitHub. These files are identified as 'CSV/XLSX not added' in the Directory Structure below

## Table of Contents

- [Overview]
- [Data Sources]
- [PostgreSQL DB & Layers]
- [Usage: Scripts and Queries execution order]
- [ETL Breakdown and their respective scripts]
- [Analytics: SQL Views and Analysis-with-Pandas]
- [Local Setup and Execution]
- [Key learnings]
- [Directory Structure]
- [Some issues faced]
- [Conclusion]

## Overview
This project was completed as part of the Arena Data Engineer Interview Preparation.
It demonstrates end-to-end data engineering concepts using real-world public datasets, focusing on:
- Data Extraction from public sources (BLS OEWS & O*NET).
- Data Transformation using Python and Pandas (cleaning, standardization, normalization).
- Data Loading into a PostgreSQL database (raw → curated layers). As well as CSV files
- Data Analysis and SQL View Creation for analytical readiness.

## Data Sources
(1)
Source: BLS OEWS (Occupational Employment and Wage Statistics)
Description: Contains annual employment and wage estimates by occupation and state.
URL: https://www.bls.gov/oes/tables.htm

(2)
Source: O*NET Skills Database
Description: Detailed skill, knowledge, and ability data by occupation (SOC codes).
URL: https://www.onetcenter.org/database.html

## PostgreSQL DB Layers
```
arena_de_prep_db
│
├── raw/
│   ├── Tables
│       ├── oews_raw                    # Directly extracted OEWS data by state
│       └── onet_skills_raw             # Extracted O*NET skills data
│
└── curated/
    ├── Tables
        ├── oews_cleaned                # OEWS data: Cleaned & standardized 
        └── onet_skills_cleaned         # O*NET data: Cleaned & standardized
    └── Views
        ├── vw_oews_avg_over_onet       # Aggregated view (6-digit → subcodes)
        └── vw_onet_closest_oews        # O*NET skills linked to parent OEWS metrics
```        

* Raw layer → stores the raw extracted data. Cleansing limited to: 
    (1)column name convention (to 'snake_case') 
    (2) string values converted to lowercase
* Curated layer → stores cleaned, transformed, and aggregated data for analytics.


## Usage: Scripts and Queries execution order
1) Load_data.py
2) eda.py
3) data_prep.py
4) SQL queries (note: run in Postgres DB)
5) analysis_pandas.py

## ETL Pipeline Breakdown and their respective Scripts
# -- Scripts and Queries - What each does

# ---- Extraction ----
1) load_data.py
- Purpose: Extract public data (BLS OEWS, O*NET), do light sanity cleaning, and load into Postgres raw schema.
- Key actions:
    - Extraction:
        - Fetch, download public datasets (ZIP/XLSX/TXT) via requests.
        - Parses with pandas (read_excel, read_csv, read_html).
        - Performs slight cleaning by converting DF columns to 'snake_case' naming convention and convert string values in the DF to lowercase
        - Writes tables with to_sql(..., schema="raw").
- Outcome: this raw but slightly cleaned data is saved in Postgres tables: raw.oews_raw, raw_onet_skills_raw tables. Also, CSV files are populated

2) eda.py (Exploratory Data Analysis)
- Purpose: explore data for familiarity. Profiles the raw data for a quick health check before heavy transforms.
- Key actions:
    - Prints row/column counts, sample rows, null distributions.
    - Validates key fields (e.g., SOC code patterns like NN-NNNN(.NN)?).
    - Flags obvious anomalies (duplicate keys, non-numeric wage fields).
- Outcome: Confidence that raw is usable; notes/TODOs for data prep.

# ---- Transformation ----
3) data_prep.py
- Purpose: Transform raw → curated (clean, standardize, and normalize).
- Key actions:
    - Creates a subset of data frame that meets requirements for analysis
    - Clean data frame data: trim whitespace in string fields, 
    - Replace non-numeric values with NULL (NaN) in numeric fields
    - Where applicable, convert data types to numeric for columns identified for aggregation (a_mean, h_mean, tot_emp, etc.).
    - Optional filters (e.g., pick geography, state) 
- Outcome: Analysis-ready curated tables with consistent typing and naming. 
    - Postgres Tables: oews_cleaned, onet_skills_cleaned
    - CSV files: oews_cleaned.csv, onet_skills_cleaned.csv

# ----- Loading -----
This step is not entirely separate. Both Extraction and Transformation phases loads to raw and curated layers respectively.
Data is loaded to two targets: 
- 1) PostgreSQL: using SQLAlchemy. A "load_df_to_postgres" function was created for reusability. 
- 2) CSV: Pandas function "df.csv" was used to save dataframes to CSV files.

Sample code loading to Postgres using SQLAlchemy. (This code is wrapped in the "load_df_to_postgres" function)
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=5000
        )


## ---- Analytics: SQL-Views and Analysis-with-Pandas
4) SQL queries (run in Postgres)
Purpose: provide analysis reports based on requirements. Create views in the curated schema that express the hierarchy logic clearly.
- 4a) curated.vw_oews_avg_over_onet
    - Goal: For each 6-digit OEWS occ_code, compute wage metrics by averaging over the set of O*NET child codes sharing that prefix.
    - Notes: Uses split_part(onet_soc_code, '.', 1) to map children → parent.

- 4b) curated.vw_onet_closest_oews
    - Goal: For each O*NET SOC (child), attach the single matching OEWS parent (6-digit) metrics.
    - Notes: Collapses OEWS to one row per occ_code (avg across areas if needed) before joining, ensuring one parent per child.

5) analysis_pandas.py
- Purpose: Read one of the views with pandas.read_sql and do a brief aggregation/visualization.
- Examples:
    - Average annual wage by SOC major group (first two digits).
    - Top-10 O*NET SOC by annual mean wage (averaged if multiple skill rows).
    - Optionally save CSVs/PNGs and print short, human-readable insights.


## Local Setup and Execution 
1. Create Virtual Environment
    1. python3 -m venv arena_venv
    2. source arena_venv/bin/activate
    3. pip install -r requirements.txt
2. PostgreSQL Setup —
    1. CREATE DATABASE arena_de_prep_db;
    2. CREATE SCHEMA raw;
    3. CREATE SCHEMA curated;
3. Environment Variables
    1. PG_URI=postgresql://<user>:<password>@localhost:5432/arena_de_prep_db
4. Run ETL Script


## Key Learnings Demonstrated
- Extracting public data (requests, pandas.read_html/read_excel)
- Schema normalization & cleaning with pandas
- Data typing and numeric coercion for aggregation
- Schema design and multi-layer architecture (raw → curated)
- SQL analytical logic for parent-child hierarchies
- Reusable and maintainable ETL design


## Directory Structure
```
arena_challenge_demo/
│
├── data_output/
│   ├── raw/                        # Saved csv extracts. # CSV not added
│   └── curated/                    # Cleaned and merged datasets (csv & PNG). # CSV not added
├── public_datasets                 # XLSX not added
│   ├── oesm24st
│   ├── related occupations.xlsx 
├── queries/
│   ├── vw_oews_avg_over_onet.sql
│   └── vw_onet_closest_oews.sql
├── scripts
│   ├── load_data.py                    # Main ETL orchestration
│   ├── eda.py                          # data profiling for exploratory data analysis
│   ├── data_prep.py                    # pre-processing and data preparation for analysis
│   ├── analysis_pandas.py              # performs user requirement's analysis using pandas
├── README.md
└── requirements.txt
```

## Some Issues Faced
- Initially scrapping with BS4 was time-consuming. Since XLSX file existed and formatted, I used that.
- Sometimes 'data_prep.py' may raise error that the function "load_df_to_postgres" does not exist, but on the second run, it works without any changes.
- Similarly, seldomly the same file may have a read_timeout. If it does, it always run on the second attempt

## Conclusion
This project demonstrates a complete mini data engineering pipeline:
- Public data ingestion
- Cleaning & transformation
- Relational modeling
- Analytical joins & aggregation views

It highlights strong fundamentals in:
```
✅ SQL design
✅ Python ETL scripting
✅ Schema organization
✅ Analytical data readiness
```