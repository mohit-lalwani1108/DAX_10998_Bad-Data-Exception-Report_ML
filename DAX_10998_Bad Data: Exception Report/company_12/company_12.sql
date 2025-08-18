-- 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- company-12 (ingestion) 

SELECT DISTINCT primary_domain, company_country AS countryCode, primary_name AS name, company_status as status, keywords from 
(
	SELECT DISTINCT * EXCEPT(naics_number, keywords_number, ceo_count,founder_count, president_count, ceo_presence_flag, naics_presence_flag ) , 
	ROW_NUMBER()OVER(PARTITION BY company_id ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN 
	from daas.dax_12136_temp_20250728 
	WHERE  keywords ILIKE '%,%' AND NULLIF(keywords,'') IS NOT NULL  --REGEXP_LIKE(keywords, '\\,' )  limit 100 
) as subquerry WHERE rn=1 
; 
  
-- 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- company-12   (qa)

SELECT * EXCEPT(RN) from
( 
	SELECT DISTINCT * EXCEPT(naics_number, keywords_number, ceo_count,founder_count, president_count, ceo_presence_flag, naics_presence_flag ) , 
	ROW_NUMBER()OVER(PARTITION BY company_id ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN
	from daas.dax_12136_temp_20250728 
	WHERE  keywords ILIKE '%,%' AND NULLIF(keywords,'') IS NOT NULL    
) as subquerry WHERE rn=1 
;
 
-- 
