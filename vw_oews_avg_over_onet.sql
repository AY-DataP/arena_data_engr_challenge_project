 /*
The goal of this analysis is to create a view that aggregates Occupational Employment and Wage Statistics (OEWS) data
across all states to compute employment-weighted average wages for each occupation (occ_code). 
This view will then be linked to O*NET SOC codes to facilitate comparisons
 */
 
CREATE OR REPLACE VIEW curated.vw_oews_avg_over_onet AS

 WITH
 
   -- Collect the list of O*NET SOC codes per 6-digit prefix (e.g., '29-1141')
onet_children AS (
  SELECT
    split_part(s.onet_soc_code, '.', 1) 	AS occ6,        --(e.g. '29-1141.01' -> '29-1141')
    s.onet_soc_code     					AS onet_soc_code
  FROM raw.onet_skills_raw s
),

-- Convert OEWS aggregate fields to numeric
oews_agg_numeric AS (
SELECT 
  occ_code,
  occ_title,
  prim_state,
  tot_emp::numeric,
  a_mean::numeric,
  h_mean::numeric
FROM curated.oews_cleaned 
),

-- Calculate weighted average to account for differences in each state total number of employees
oews_weighted AS (
  SELECT
  o.occ_code,
  MIN(o.occ_title) 	AS occ_title,
  SUM(o.tot_emp) 	AS total_emp_sum,
  AVG(o.tot_emp) 	AS total_emp_avg,
  SUM(o.a_mean * o.tot_emp) / NULLIF(SUM(o.tot_emp), 0) AS weighted_annual_mean_wage,
  SUM(o.h_mean * o.tot_emp) / NULLIF(SUM(o.tot_emp), 0) AS weighted_hourly_mean_wage
  FROM oews_agg_numeric o -- view
  GROUP BY o.occ_code
),

-- Combine views from above
expanded AS (
  SELECT
    ow.*,
    oc.onet_soc_code
  FROM oews_weighted ow
  JOIN onet_children oc
    ON oc.occ6 = ow.occ_code
  JOIN oews_agg_numeric oa 
  	ON oa.occ_code = ow.occ_code
)

-- Final single select statement
SELECT
  	occ_code,
	onet_soc_code,
    MIN(occ_title)                   		   AS occ_title, 
    ROUND(MAX(total_emp_sum), 2)               AS total_emp_sum,
    ROUND(MAX(total_emp_avg), 2)               AS total_emp_avg, 
    ROUND(MAX(weighted_annual_mean_wage), 2)   AS weighted_annual_mean_wage,
 	ROUND(MAX(weighted_hourly_mean_wage), 2)   AS weighted_hourly_mean_wage
	
FROM expanded
WHERE 1 = 1 
-- AND occ_code = '29-1141'
GROUP BY occ_code, onet_soc_code
ORDER BY occ_code, onet_soc_code
;