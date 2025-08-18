--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- company-7  (ingestion) 

SELECT DISTINCT primary_domain AS domain , company_country AS countryCode, primary_name AS name, company_status as status  from 
(
	SELECT DISTINCT * EXCEPT(naics_number, keywords_number, ceo_count,founder_count, president_count, ceo_presence_flag, naics_presence_flag ) , 
	ROW_NUMBER()OVER(PARTITION BY company_id ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN 
	from daas.dax_12136_temp_20250728 
	WHERE LENGTH(NULLIF(primary_name,''))>25 AND NOT REGEXP_LIKE(primary_name, ' ')  
) as subquerry WHERE rn=1 AND LOWER( primary_name ) <> LOWER(SPLIT_PART( primary_domain  , '.',1))
;

--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- company-7 (qa) 

SELECT DISTINCT * EXCEPT(RN) from 
( 
	SELECT DISTINCT * EXCEPT(naics_number, keywords_number, ceo_count,founder_count, president_count, ceo_presence_flag, naics_presence_flag ) , 
	ROW_NUMBER()OVER(PARTITION BY company_id ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN 
	from daas.dax_12136_temp_20250728 
	WHERE LENGTH(NULLIF(primary_name,''))>25 AND NOT REGEXP_LIKE(primary_name, ' ')  
) as subquerry WHERE rn=1 AND LOWER( primary_name ) <> LOWER(SPLIT_PART( primary_domain  , '.',1))
;
 
-- 
