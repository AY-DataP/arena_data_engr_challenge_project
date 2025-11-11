# ================================================================
# Description: Analyze O*NET-OEWS view with pandas.
# This script connects to a Postgres database to read a view that joins O*NET skills to OEWS wages,
# performs analyses using pandas, and saves results and plots.
# =================================================================

# ========= Import necessary libraries ==========
import os
import re
import pandas as pd
import matplotlib.pyplot as plt
from typing import Optional, Dict, List
from sqlalchemy import create_engine


# ========= Set up Postgres connection =========
# ----- set Postgres connection URI from environment variable ----
PG_URI = os.getenv("PG_URI")
if not PG_URI:
    raise ValueError("Environment variable 'PG_URI' is not set.")


# ======== Analyze O*NET-OEWS view function =========
    # What it does:
    #   1) Reads the view with pandas.read_sql (via SQLAlchemy engine).
    #   2) Aggregates average wage by SOC major group (first two digits).
    #   3) Finds Top-10 O*NET SOC codes by annual mean wage.
    #   4) Plots both results with Matplotlib and saves PNGs.
    #   5) Provides a brief textual interpretation of findings.
    
    # Function Parameters
    # ----------
    # pg_uri : str
    #     SQLAlchemy-style Postgres URI, 
    # view_name : str
    #     Fully qualified view name to read. Default "curated.vw_onet_closest_oews".
    # save_dir : str, optional
    #     If provided, save CSVs and PNG charts in this directory.
    # show_plots : bool
    #     If True, display charts interactively (useful in notebooks).
    
    # Output
    # -------
    # dict[str, pd.DataFrame]
    #     {
    #       "raw": <raw view dataframe>,
    #       "avg_wage_by_major_group": <DF>,
    #       "top10_soc_by_wage": <DF>
    #     }

    # Notes
    # -----
    # - Expected columns in the view:
    #     O*NET side:  'soc_code' (e.g., '29-1141.01'), 'soc_title', 'skill_id', ...
    #     OEWS side:   wage columns such as 'a_mean' (or 'oews_a_mean'), 'tot_emp', etc.
    #   This function looks for 'a_mean' first, then 'oews_a_mean'.

def analyze_onet_oews_view(
    pg_uri: str,
    view_name: str = "curated.vw_onet_closest_oews",
    *,
    save_dir: Optional[str] = None,
    show_plots: bool = False
) -> Dict[str, pd.DataFrame]:
    # -------------------------
    # 1) Connect & read the view
    # -------------------------
    engine = create_engine(PG_URI)
    sql = f"SELECT * FROM {view_name};"
    df = pd.read_sql(sql, engine)

    # sanity check: ensure data exists
    if df.empty:
        raise ValueError(
            f"No rows returned from {view_name}. "
            "Confirm the view exists and your connection has access."
        )

    # Determine which wage column to use (defensive)
    wage_col_candidates = [c for c in ["a_mean", "oews_a_mean"] if c in df.columns]
    if not wage_col_candidates:
        raise ValueError(
            "Could not find a wage column. Expected one of: 'a_mean', 'oews_a_mean'. "
            f"Available columns: {list(df.columns)}"
        )
    wage_col = wage_col_candidates[0]

    # Convert/ensure wage column is numeric. IF empty, convert to missing values (NaN)
    df[wage_col] = pd.to_numeric(df[wage_col], errors="coerce")

    # Ensure we have a SOC code column from O*NET side
    soc_col_candidates = [c for c in ["soc_code", "onet_soc_code"] if c in df.columns]
    if not soc_col_candidates:
        raise ValueError(
            "Could not find SOC code column. Expected 'soc_code' or 'onet_soc_code'."
        )
    soc_col = soc_col_candidates[0]

    # -----------------------------------------
    # 2) Average wage by SOC major group (00-99)
    # -----------------------------------------
    # Extract the first two digits of the O*NET code's SOC prefix as the "major group".
    # Example: "29-1141.01" -> "29" (Healthcare practitioners/technical occupations).
    def major_group(s: str) -> Optional[str]:
        if not isinstance(s, str):
            return None
        m = re.match(r"^(\d{2})-", s)  # grabs "29" from "29-1141.01"
        return m.group(1) if m else None

    df["soc_major_group"] = df[soc_col].map(major_group)

    avg_wage_by_major = (
        df
        .groupby("soc_major_group", dropna=True, as_index=False)[wage_col]
        .mean()
        .rename(columns={wage_col: "avg_annual_mean_wage"})
        .sort_values("avg_annual_mean_wage", ascending=False)
    )

    # ---------------------------------------------------
    # 3) Top-10 O*NET SOC codes by annual mean wage (avg)
    # ---------------------------------------------------
    # Aggregate to one row per O*NET SOC code (averaging in case the view has multiple skill rows per code)
    top10_soc = (
        df.groupby(soc_col, as_index=False)[wage_col]
          .mean()
          .rename(columns={wage_col: "avg_annual_mean_wage"})
          .sort_values("avg_annual_mean_wage", ascending=False)
          .head(10)
    )

    # -------------------------------
    # 4) Save CSVs and plots
    # -------------------------------
    if save_dir:
        os.makedirs(save_dir, exist_ok=True)
        raw_path = os.path.join(save_dir, "vw_onet_closest_oews_raw.csv")
        avg_path = os.path.join(save_dir, "avg_wage_by_major_group.csv")
        top_path = os.path.join(save_dir, "top10_soc_by_wage.csv")
        df.to_csv(raw_path, index=False)
        avg_wage_by_major.to_csv(avg_path, index=False)
        top10_soc.to_csv(top_path, index=False)

        # Charts (simple defaults; no custom colors per your style guidelines)
        plt.figure()
        plt.bar(avg_wage_by_major["soc_major_group"].astype(str),
                avg_wage_by_major["avg_annual_mean_wage"])
        plt.title("Average Annual Mean Wage by SOC Major Group")
        plt.xlabel("SOC Major Group (first two digits)")
        plt.ylabel("Average Annual Mean Wage")
        plt.tight_layout()
        plt_path1 = os.path.join(save_dir, "avg_wage_by_major_group.png")
        plt.savefig(plt_path1)
        if show_plots: plt.show()
        plt.close()

        plt.figure()
        # For readability on x-axis, use short labels
        plt.bar(top10_soc[soc_col].astype(str), top10_soc["avg_annual_mean_wage"])
        plt.title("Top-10 O*NET SOC Codes by Annual Mean Wage (average)")
        plt.xlabel("O*NET SOC Code")
        plt.ylabel("Average Annual Mean Wage")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        plt_path2 = os.path.join(save_dir, "top10_soc_by_wage.png")
        plt.savefig(plt_path2)
        if show_plots: plt.show()
        plt.close()

        print(f"Saved CSVs to: {save_dir}")
        print(f"Saved charts to: {save_dir}")

    # ---------------------------------------
    # 5) Brief, human-readable interpretations
    # ---------------------------------------
    # A) Which major group appears highest?
    if not avg_wage_by_major.empty:
        top_group_row = avg_wage_by_major.iloc[0]
        print(
            f"• Highest average wage major group: SOC {top_group_row['soc_major_group']} "
            f"with ~${top_group_row['avg_annual_mean_wage']:,.0f} average annual mean wage."
        )

    # B) Which O*NET SOC codes show up in the top-10 list?
    if not top10_soc.empty:
        sample_codes = ", ".join(top10_soc[soc_col].head(5).tolist())
        max_row = top10_soc.iloc[0]
        print(
            f"• Top wage O*NET SOC code: {max_row[soc_col]} "
            f"with ~${max_row['avg_annual_mean_wage']:,.0f}. "
            f"Others in top set include: {sample_codes}."
        )

    # C) Caveat about interpretation (important for interviews)
    print(
        "• Note: Wages come from OEWS at the 6-digit SOC level. O*NET children inherit the parent’s wage; "
        "averages here reflect grouping structure rather than true sub-occupation wage differentiation."
    )

    return {
        "raw": df,
        "avg_wage_by_major_group": avg_wage_by_major,
        "top10_soc_by_wage": top10_soc,
    }



# ============= Call the above function ==============
# This section describes how to call the function above.
# Uncomment and modify the following lines as needed.
# ===========================================================

results = analyze_onet_oews_view(
    pg_uri=PG_URI,
    view_name="curated.vw_onet_closest_oews",
    save_dir="data_output/curated",
    show_plots=False
) 

# -------- Access the dataframes if you need them later --------
# avg_by_group = results["avg_wage_by_major_group"]
# top10 = results["top10_soc_by_wage"]
# print(top10)