--- VIEW to get average oews data by onet soc code

CREATE OR REPLACE VIEW vw_oews_avg_over_onet AS 
WITH cte1 AS (
	SELECT *,  split_part(onet_soc_code, '.', 1) as split_onet_soc_code
	FROM onet_skills
)

SELECT split_onet_soc_code, ROUND(AVG(data_value), 2) as mean_data_value,
	ROUND(AVG(standard_error), 2) as mean_sd,
	ROUND(AVG(lower_ci_bound), 2) as mean_lower_ci_bound,
	ROUND(AVG(upper_ci_bound), 2) as mean_upper_ci_bound
FROM cte1
GROUP BY split_onet_soc_code

--- VIEW to join onet skills with oews by state

CREATE OR REPLACE VIEW vw_onet_closest_oews AS 
WITH cte1 AS (
	SELECT *,  split_part(onet_soc_code, '.', 1) as split_onet_soc_code
	FROM onet_skills
)

SELECT *
FROM cte1 t1
JOIN oews_by_state t2
ON t1.split_onet_soc_code = t2.soc_code