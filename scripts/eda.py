# ==================================================================
# Description: This script performs Exploratory Data Analysis (EDA) on the OEWS and ONET Skills datasets
#   using the ydata-profiling library to generate comprehensive profiling reports.
# ==================================================================

# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Dependency Installation: ydata-profiling (newer) or pandas-profiling (original)
# pip install ydata-profiling pandas-profiling     
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

# ========== Import necessary libraries ==========
import pandas as pd
from ydata_profiling import ProfileReport


# +++++++ EDA Profiling Report Generation +++++++

# 1. ------- Importing dataset: OEWS Dataset
oews_df = pd.read_csv('data_output/raw/oews_raw_df.csv') 

# 2. ------- Generate the Profile Report object 
oews_profile = ProfileReport(
    oews_df, 
    title="OEWS Profiling Report",      # ----- gives the report a custom title
    explorative=True                    # ----- calculates more descriptive statistics for a deeper dive
) 

# 3. ------- Save report as HTML file
oews_profile.to_file("oews_profiling_report.html")


# ************ Repeat the above steps for the ONET Skills dataset ************

# 1. ------- Importing dataset
onet_skills_df = pd.read_csv('data_output/raw/onet_skills_raw_df.csv') 

# 2. ------- Generate the Profile Report object
onet_skills_profile = ProfileReport(
    onet_skills_df, 
    title="ONET SKills Profiling Report",   # ----- gives the report a custom title
    explorative=True                        # ----- calculates more descriptive statistics for a deeper dive
) 

# 3. ------- Save report as HTML file
onet_skills_profile.to_file("onet_skills_profiling_report.html")