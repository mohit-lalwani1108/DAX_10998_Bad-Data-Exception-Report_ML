-- 
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------  
-- company-21  (ingestion) 

SELECT DISTINCT primary_domain AS domain , company_country AS countryCode, primary_name AS name, company_status as status,linkedin_url from 
(
	SELECT DISTINCT * EXCEPT(naics_number, keywords_number, ceo_count,founder_count, president_count, ceo_presence_flag, naics_presence_flag ) , 
	ROW_NUMBER()OVER(PARTITION BY company_id ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN  
	from daas.dax_12136_temp_20250728 
	WHERE linkedin_url NOT ILIKE '%linkedin.com%' 
) as subquerry
WHERE rn=1 
;  
 
-- 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- company-21  (qa) 
	
SELECT * EXCEPT(RN) from 
( 
	SELECT DISTINCT * EXCEPT(naics_number, keywords_number, ceo_count,founder_count, president_count, ceo_presence_flag, naics_presence_flag ) , 
	ROW_NUMBER()OVER(PARTITION BY company_id ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN  
	from daas.dax_12136_temp_20250728 
	WHERE linkedin_url NOT ILIKE '%linkedin.com%'  
) as subquerry WHERE rn=1   
; 
 
-- 
