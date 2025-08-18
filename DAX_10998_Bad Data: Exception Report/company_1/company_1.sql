--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- company-1 (ingestion) 

SELECT DISTINCT primary_domain as domain , company_country AS countryCode, primary_name AS name, company_status as status, ROUND(NULLIF(revenue, '0')::DOUBLE*1E6) as revenue from daas.dax_12136_temp_20250728 
WHERE ROUND(NULLIF(revenue, '0')::DOUBLE*1E6)  > 775000000000 
;

--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- company-1 (qa)

SELECT * EXCEPT(RN) from
(
	SELECT DISTINCT * EXCEPT(naics_number, keywords_number, ceo_count,founder_count, president_count, ceo_presence_flag, naics_presence_flag ) , 
	ROW_NUMBER()OVER(PARTITION BY company_id ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN
	from daas.dax_12136_temp_20250728
	WHERE ROUND(NULLIF(revenue, '0')::DOUBLE*1E6)  > 775000000000  
) where rn = 1
; 

--
