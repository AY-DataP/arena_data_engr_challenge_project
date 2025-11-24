/*
This analysis is focused on comparing wage data for specific occupations (occ_code) at the state level against the
employment-weighted average wage for that occupation across all states.
*/

CREATE OR REPLACE VIEW curated.vw_oews_state_vs_weighted AS

WITH
-- 1) State-level metrics per occ_code
state_stats AS (
  SELECT
    occ_code,
    prim_state,
    MIN(occ_title) AS occ_title,

    -- state employment and simple state averages
    SUM(tot_emp) AS state_tot_emp,
    AVG(a_mean)  AS state_a_mean,
    AVG(h_mean)  AS state_h_mean
  FROM curated.oews_cleaned
  GROUP BY occ_code, prim_state
),

-- 2) Overall weighted metrics per occ_code (across all states)
weighted_stats AS (
  SELECT
    occ_code,
    MIN(occ_title) AS occ_title,
    SUM(tot_emp) AS total_emp_all_states,
	AVG(tot_emp) AS avg_emp_all_states,
    -- employment-weighted mean wages
    SUM(a_mean * tot_emp) / NULLIF(SUM(tot_emp), 0) AS weighted_a_mean,
    SUM(h_mean * tot_emp) / NULLIF(SUM(tot_emp), 0) AS weighted_h_mean
  FROM curated.oews_cleaned
  GROUP BY occ_code
)

-- 3) Join state results to overall weighted results
SELECT
  s.occ_code,
  s.occ_title,
  s.prim_state,

  s.state_tot_emp,
  s.state_a_mean,
  s.state_h_mean,

  w.total_emp_all_states,
  ROUND(w.avg_emp_all_states::NUMERIC, 2) AS avg_emp_all_states,
  ---------------------
-- Weighted Means 
ROUND(w.weighted_a_mean::NUMERIC, 2) AS weighted_annual_mean_wage,
ROUND(w.weighted_h_mean::NUMERIC, 2) AS weighted_hourly_mean_wage,

-- Comparisons 
ROUND((s.state_a_mean - w.weighted_a_mean)::NUMERIC, 2) AS diff_annual_mean_wage,
ROUND((s.state_h_mean - w.weighted_h_mean)::NUMERIC, 2) AS diff_hourly_mean_wage,

-- Ratio
CASE WHEN w.weighted_a_mean > 0
     THEN ROUND((s.state_a_mean / w.weighted_a_mean)::NUMERIC, 2)
END AS ratio_annual_mean_wage,

CASE WHEN w.weighted_h_mean > 0
     THEN ROUND((s.state_h_mean / w.weighted_h_mean)::NUMERIC, 2)
END AS ratio_hourly_mean_wage
  ---------------------

FROM state_stats s
JOIN weighted_stats w
  ON s.occ_code = w.occ_code
WHERE 1 = 1
AND w.occ_code != '00-0000'
-- AND w.occ_code = '29-1141'
-- AND prim_state = 'md'
ORDER BY s.occ_code, s.prim_state
;
