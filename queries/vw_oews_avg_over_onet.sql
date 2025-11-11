 CREATE OR REPLACE VIEW curated.vw_oews_avg_over_onet AS
 WITH
   -- 1) Collect the list of O*NET SOC codes per 6-digit prefix (e.g., '29-1141')
onet_children AS (
  SELECT
    split_part(s.onet_soc_code, '.', 1) 	AS occ6,        -- '29-1141.01' -> '29-1141'
    s.onet_soc_code     					AS onet_soc_code
  FROM raw.onet_skills_raw s
--   GROUP BY 1
)

SELECT
  	o.occ_code,
	c.onet_soc_code,
    min(o.occ_title)     AS occ_title, 
    avg(o.tot_emp)       AS total_employment_avg,
    avg(o.jobs_1000)     AS num_jobs_per_1000_avg, 
    avg(o.mean_prse)     AS mean_prse_avg,
 	avg(o.pct_total)     AS pct_total_avg,
 	avg(o.pct_rpt)       AS pct_rpt_avg,

    -- annual wages
    avg(o.a_mean)        AS annual_mean_wage_avg,
    avg(o.a_median)      AS annual_median_wage_avg,
    avg(o.a_pct10)       AS a_pct10_avg,
    avg(o.a_pct25)       AS a_pct25_avg,
    avg(o.a_pct75)       AS a_pct75_avg,
    avg(o.a_pct90)       AS a_pct90_avg,

    -- hourly wages
    avg(o.h_mean)        AS hourly_wage_avg,
    avg(o.h_median)      AS hourly_median_wage_avg,
    avg(o.h_pct10)       AS h_pct10_avg,
    avg(o.h_pct25)       AS h_pct25_avg,
    avg(o.h_pct75)       AS h_pct75_avg, 
    avg(o.h_pct90)       AS h_pct90_avg,
	
-- 	other fields for filtering -----:
--  	o.prim_state, 		-- commented out	

    -- if True, then annual & hourly wage were released. Otherwise, not relased.
     CASE WHEN annual IN ('TRUE','true','t','1')  THEN 1 ELSE 0 END AS annual_flag,
     CASE WHEN hourly IN ('TRUE','true','t','1')  THEN 1 ELSE 0 END AS hourly_flag
  
  FROM curated.oews_cleaned o
  JOIN onet_children c
    ON c.occ6 = o.occ_code
  WHERE 1 = 1 
  AND o.prim_state = 'md'    -- Maryland level only
  GROUP BY occ_code, onet_soc_code, annual, hourly --, prim_state
  ORDER BY occ_code, onet_soc_code