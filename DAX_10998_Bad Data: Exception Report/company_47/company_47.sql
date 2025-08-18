--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--  Company 47 (Ingestion)

SELECT primary_domain AS domain, primary_name as name ,company_country as countryCode, company_status as status  
FROM daas.dax_12136_temp_20250728 WHERE
(
	primary_name ILIKE '%aquired by%' 
	OR REGEXP_LIKE(primary_name, '\\b(?i)doing business as\\b' )
	OR REGEXP_LIKE(primary_name, '\\b(?i)previously ?known ?as\\b' ) 
	OR REGEXP_LIKE(primary_name, '\\b(?i)previous|known ?as|formerly|aquisition ? of|an ?aquisition ?of|owned ?by|merged ?with|aquired ?by|joined|formerly known ?as|previously ?known ?as|renamed? ?to|transitioned ?to|changed ?name ?to|trading as|closed|permanently closed|shut ?down\\b' )                    
	OR REGEXP_LIKE(primary_name, '(i?)\\bdba\\b' )  
	) 
AND LOWER(TRIM((SPLIT_PART(primary_domain , '.', 1)))) <> LOWER(TRIM(primary_name))
;

--
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--  Company 47 ( QA ) 

SELECT * EXCEPT(RN) FROM
(
	SELECT * EXCEPT(naics_number, keywords_number,founder_count, president_count, ceo_presence_flag, naics_presence_flag ), ROW_NUMBER()OVER(PARTITION BY company_id ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN 
	FROM daas.dax_12136_temp_20250728 WHERE 
	(
		primary_name ILIKE '%aquired by%' 
		OR REGEXP_LIKE(primary_name, '\\b(?i)doing business as\\b' )
		OR REGEXP_LIKE(primary_name, '\\b(?i)previously ?known ?as\\b' ) 
		OR REGEXP_LIKE(primary_name, '\\b(?i)previous|known ?as|formerly|aquisition ? of|an ?aquisition ?of|owned ?by|merged ?with|aquired ?by|joined|formerly known ?as|previously ?known ?as|renamed? ?to|transitioned ?to|changed ?name ?to|trading as|closed|permanently closed|shut ?down\\b' )                    
		OR REGEXP_LIKE(primary_name, '(i?)\\bdba\\b' )  
		) 
	AND LOWER(TRIM((SPLIT_PART(primary_domain , '.', 1)))) <> LOWER(TRIM(primary_name))
) AS subquerry
;

-- 
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--
