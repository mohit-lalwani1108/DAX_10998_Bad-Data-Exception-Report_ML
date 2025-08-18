-- 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- company-8   (ingestion) 

WITH CTE
(
	SELECT primary_domain , company_country , EXPLODE(SPLIT(naics, '\\|')) AS naics 
	from
	(
		SELECT * FROM daas.dax_12136_temp_20250728
	) as subquerry 
	GROUP BY ALL 
) ,  
CTE2 
(
	SELECT primary_domain as domain , company_country as country, LENGTH(naics) AS length , COUNT(DISTINCT naics) AS counts
	FROM CTE 
	GROUP BY ALL 
	ORDER BY 1,2  
), 
CTE3 AS 
(
	SELECT DISTINCT * FROM CTE2 
	WHERE counts > 6 
),  
summary AS (
  SELECT
    domain,
    country,
    MAX(CASE WHEN length = 2 THEN counts ELSE NULL END) AS count_2,
    MAX(CASE WHEN length = 4 THEN counts ELSE NULL END) AS count_4,
    MAX(CASE WHEN length = 6 THEN counts ELSE NULL END) AS count_6,
    COUNT(DISTINCT CASE WHEN length = 2 THEN 1 END) AS has_2,
    COUNT(DISTINCT CASE WHEN length = 4 THEN 1 END) AS has_4,
    COUNT(DISTINCT CASE WHEN length = 6 THEN 1 END) AS has_6
  FROM CTE3
  GROUP BY domain, country
),
filtered AS (
  SELECT domain, country
  FROM summary
  WHERE
    -- Case: length 6 exists and has highest count
    (has_6 = 1 AND (count_6 > COALESCE(count_2, 0)) AND (count_6 > COALESCE(count_4, 0)))
    OR
    -- Case: only 2 and 4 exist, and 4 > 6 (i.e., 6 not present)
    (has_2 = 1 AND has_4 = 1 AND has_6 = 0 AND count_4 > COALESCE(count_6, 0))
)
SELECT DISTINCT T2.primary_domain , T2.company_country AS countryCode, T2.primary_name as name , T2.company_status as status, T2.naics FROM filtered AS T1 
JOIN daas.dax_12136_temp_20250728 T2
ON T1.domain = T2.primary_domain AND T1.country = T2.company_country 
;  

--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- company-8  (qa)

WITH CTE 
(
	SELECT primary_domain , company_country , EXPLODE(SPLIT(naics, '\\|')) AS naics 
	from
	(
		SELECT * FROM daas.dax_12136_temp_20250728  
	) as subquerry 
	GROUP BY ALL 
) ,  
CTE2 
(
	SELECT primary_domain as domain , company_country as country, LENGTH(naics) AS length , COUNT(DISTINCT naics) AS counts
	FROM CTE 
	GROUP BY ALL 
	ORDER BY 1,2  
), 
CTE3 AS 
(
	SELECT DISTINCT * FROM CTE2 
	WHERE counts > 6 
),  
summary AS (
  SELECT
    domain,
    country,
    MAX(CASE WHEN length = 2 THEN counts ELSE NULL END) AS count_2,
    MAX(CASE WHEN length = 4 THEN counts ELSE NULL END) AS count_4,
    MAX(CASE WHEN length = 6 THEN counts ELSE NULL END) AS count_6,
    COUNT(DISTINCT CASE WHEN length = 2 THEN 1 END) AS has_2,
    COUNT(DISTINCT CASE WHEN length = 4 THEN 1 END) AS has_4,
    COUNT(DISTINCT CASE WHEN length = 6 THEN 1 END) AS has_6
  FROM CTE3
  GROUP BY domain, country
),
filtered AS (
  SELECT domain, country
  FROM summary
  WHERE
    -- Case: length 6 exists and has highest count
    (has_6 = 1 AND (count_6 > COALESCE(count_2, 0)) AND (count_6 > COALESCE(count_4, 0)))
    OR
    -- Case: only 2 and 4 exist, and 4 > 6 (i.e., 6 not present)
    (has_2 = 1 AND has_4 = 1 AND has_6 = 0 AND count_4 > COALESCE(count_6, 0))
)
SELECT DISTINCT T2.* EXCEPT(naics_number, keywords_number, ceo_count,founder_count, president_count, ceo_presence_flag, naics_presence_flag )  FROM filtered AS T1 
JOIN daas.dax_12136_temp_20250728 T2 
ON T1.domain = T2.primary_domain AND T1.country = T2.company_country 
;

