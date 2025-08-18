--
------------------------,--------------------------------------------------------------------------------------------------------------------------------------------------
-- COMPANY - 35 Ingestion 

SELECT DISTINCT primary_domain as domain, company_country AS countryCode, primary_name AS name, company_status as status, employee_count, revenue
from
(
	SELECT DISTINCT * EXCEPT(naics_number, keywords_number,founder_count, president_count, ceo_presence_flag, naics_presence_flag ) , 
	ROW_NUMBER()OVER(PARTITION BY company_id ORDER BY null) AS RN 
	FROM daas.dax_12136_temp_20250728 
	where employee_count in(0,1) 
	AND ROUND(NULLIF(revenue, '0')::DOUBLE * 1E6) > 10000000
) as subquerry WHERE RN = 1  
;

--
------------------------,--------------------------------------------------------------------------------------------------------------------------------------------------
-- COMPANY 35 QA

SELECT * EXCEPT(RN) FROM 
(
	SELECT DISTINCT * EXCEPT(naics_number, keywords_number,founder_count, president_count, ceo_presence_flag, naics_presence_flag ) , 
	ROW_NUMBER()OVER(PARTITION BY company_id ORDER BY null) AS RN 
	FROM daas.dax_12136_temp_20250728 
	where employee_count in(0,1) 
	AND ROUND(NULLIF(revenue, '0')::DOUBLE * 1E6) > 10000000
)
;

-- 
