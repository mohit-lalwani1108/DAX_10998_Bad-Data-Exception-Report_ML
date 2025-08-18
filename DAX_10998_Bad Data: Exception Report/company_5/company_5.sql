--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- company-5 (ingestion) 
 
SELECT DISTINCT primary_domain AS domain, company_country AS countryCode, primary_name AS name, company_status as status, employee_count,ROUND(NULLIF(revenue, '0')::DOUBLE*1E6) as revenue from 
( 
	SELECT DISTINCT * EXCEPT(naics_number, keywords_number, ceo_count,founder_count, president_count, ceo_presence_flag, naics_presence_flag ) , 
	ROW_NUMBER()OVER(PARTITION BY company_id ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN 
	from daas.dax_12136_temp_20250728 
	WHERE NULLIF(employee_count,0) <= 500 AND ROUND(NULLIF(revenue, '0')::DOUBLE*1E6)  >= 1000000000 
) as subquerry WHERE rn=1 
;  

--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- company-5 (qa) 

SELECT * EXCEPT(RN) from
( 
	SELECT DISTINCT * EXCEPT(naics_number, keywords_number, ceo_count,founder_count, president_count, ceo_presence_flag, naics_presence_flag ) , 
	ROW_NUMBER()OVER(PARTITION BY company_id ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN
	from daas.dax_12136_temp_20250728 
	WHERE NULLIF(employee_count,0) <= 500 AND  ROUND(NULLIF(revenue, '0')::DOUBLE*1E6)  >= 1000000000 
) as subquerry WHERE rn=1
; 
  
--
