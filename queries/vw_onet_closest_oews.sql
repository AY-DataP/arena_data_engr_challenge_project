-- Create a curated view that maps each O*NET SOC child to its closest OEWS parent
CREATE OR REPLACE VIEW curated.vw_onet_closest_oews AS
WITH oews_one_row AS (
  -- Collapse OEWS to exactly one row per 6-digit occ_code.
  SELECT
    o.occ_code,
	prim_state 		   AS prim_state,	
    MIN(o.occ_title)   AS occ_title,
    AVG(o.tot_emp)     AS tot_emp,
    AVG(o.jobs_1000)   AS jobs_1000,
    AVG(o.mean_prse)   AS mean_prse,
    -- annual wages
    AVG(o.a_mean)      AS a_mean,
    AVG(o.a_median)    AS a_median,
    AVG(o.a_pct10)     AS a_pct10,
    AVG(o.a_pct25)     AS a_pct25,
    AVG(o.a_pct75)     AS a_pct75,
    AVG(o.a_pct90)     AS a_pct90,
    -- hourly wages
    AVG(o.h_mean)      AS h_mean,
    AVG(o.h_median)    AS h_median,
    AVG(o.h_pct10)     AS h_pct10,
    AVG(o.h_pct25)     AS h_pct25,
    AVG(o.h_pct75)     AS h_pct75,
    AVG(o.h_pct90)     AS h_pct90
  FROM curated.oews_cleaned o
  GROUP BY o.occ_code, prim_state
)

SELECT
  -- O*NET (skills) columns
  s.onet_soc_code                        AS onet_soc_code,
  s.title                                AS onet_job_title,  
  s.element_id                           AS skill_id,
  s.element_name                         AS skill_description,
  s.scale_id							 AS proficiency_lvl_id,	

  -- Attached OEWS parent (6-digit) wage/employment metrics
  o.occ_code                              AS oews_occ_code,
  o.occ_title                             AS oews_occ_title,
  o.prim_state,
  o.tot_emp,
  o.jobs_1000,
  o.mean_prse,
  o.a_mean, o.a_median, o.a_pct10, o.a_pct25, o.a_pct75, o.a_pct90,
  o.h_mean, o.h_median, o.h_pct10, o.h_pct25, o.h_pct75, o.h_pct90

FROM raw.onet_skills_raw s
LEFT JOIN oews_one_row o
  ON o.occ_code = split_part(s.onet_soc_code, '.', 1)   -- Match O*NET 6-digit prefix with OEWS occ_code
WHERE 1 = 1 
 AND o.prim_state = 'md'
 AND scale_id = 'im'
--  AND hourly = '1' -- (1 = TRUE, 0 = FALSE)
--  AND annual = '1' -- (1 = TRUE, 0 = FALSE)
ORDER BY s.onet_soc_code, s.element_id, o.prim_state, s.scale_id;