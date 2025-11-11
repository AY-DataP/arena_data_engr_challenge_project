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