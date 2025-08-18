--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- company-3 (ingestion) 

SELECT DISTINCT primary_domain as domain , company_country AS countryCode, primary_name AS name, company_status as status, employee_count from 
(
	SELECT DISTINCT * EXCEPT(naics_number, keywords_number, ceo_count,founder_count, president_count, ceo_presence_flag, naics_presence_flag ) , 
	ROW_NUMBER()OVER(PARTITION BY company_id ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN
	from daas.dax_12136_temp_20250728 
	WHERE NULLIF(employee_count,0) <= 20 AND  HV_count > 5 
) AS subquerry where RN = 1 
;

--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- company-3 (qa) 

SELECT * EXCEPT(RN) FROM
(
	SELECT DISTINCT * EXCEPT(naics_number, keywords_number, ceo_count,founder_count, president_count, ceo_presence_flag, naics_presence_flag ) , 
	ROW_NUMBER()OVER(PARTITION BY company_id ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN
	from daas.dax_12136_temp_20250728 
	WHERE NULLIF(employee_count,0) <= 20 AND  HV_count > 5 
) AS SUBQERRY WHERE RN = 1 
;

--
