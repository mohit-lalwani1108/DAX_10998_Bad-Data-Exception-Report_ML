-- 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- company-32   (ingestion) 
 
SELECT DISTINCT primary_domain as domain, company_country AS countryCode, primary_name AS name, company_status as status, employee_count
FROM
(
    SELECT DISTINCT * EXCEPT(naics_number, keywords_number, founder_count, president_count, ceo_presence_flag, naics_presence_flag),  
    ROW_NUMBER()OVER(PARTITION BY company_id ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN , 
    EXPLODE(SPLIT(naics, '\\|' )) as explode_naics,REGEXP_COUNT(naics, '\\|' )
    FROM daas.dax_12136_temp_20250728 
    WHERE  NULLIF(naics, '' ) IS NOT NULL 
--    limit 1000 
) AS subquerry WHERE RN = 1 AND explode_naics NOT IN ( SELECT DISTINCT `Naics Codes` FROM si_dataops_prod.daas.temp_table_department_size_ml ) 
; 

-- 
------------------------,-------------------------------------------------------------------------------------------------------------------------------------------------- 
-- company-32  (qa) 
 
SELECT * EXCEPT(RN) FROM
( 
    SELECT DISTINCT * EXCEPT(naics_number, keywords_number, founder_count, president_count, ceo_presence_flag, naics_presence_flag),  
    ROW_NUMBER()OVER(PARTITION BY company_id ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN , 
    EXPLODE(SPLIT(naics, '\\|' )) as explode_naics,REGEXP_COUNT(naics, '\\|' )
    FROM daas.dax_12136_temp_20250728 
    WHERE  NULLIF(naics, '' ) IS NOT NULL 
) as subquerry WHERE RN = 1 AND explode_naics NOT IN ( SELECT DISTINCT `Naics Codes` FROM si_dataops_prod.daas.temp_table_department_size_ml )  
; 

--
