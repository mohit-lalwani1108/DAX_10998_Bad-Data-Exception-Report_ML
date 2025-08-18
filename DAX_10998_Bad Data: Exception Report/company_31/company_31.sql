-- 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- company-31   (ingestion)

SELECT DISTINCT primary_domain as domain, company_country AS countryCode, primary_name AS name, company_status as status, employee_count
from
(
    SELECT DISTINCT * EXCEPT(naics_number, keywords_number, founder_count, president_count, ceo_presence_flag, naics_presence_flag),  
    ROW_NUMBER()OVER(PARTITION BY company_id ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN
    FROM daas.dax_12136_temp_20250728
    WHERE employee_count > 50000 and NULLIF( linkedin_url, '') IS NULL
) as subquerry WHERE RN = 1 
;

-- 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- company-31  (qa) 
 
SELECT * EXCEPT(RN) FROM 
(
	   SELECT DISTINCT * EXCEPT(naics_number, keywords_number, founder_count, president_count, ceo_presence_flag, naics_presence_flag),  
    ROW_NUMBER()OVER(PARTITION BY company_id ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN 
    FROM daas.dax_12136_temp_20250728 
    WHERE employee_count > 50000 and NULLIF( linkedin_url, '') IS NULL 
) as subquerry WHERE RN = 1 
;  

-- 
