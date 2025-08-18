--
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- COMPANY 39 INGESTION

SELECT DISTINCT primary_domain as domain , company_country AS countryCode, primary_name AS name, company_status as status,employee_count
from
(   
	SELECT DISTINCT * EXCEPT(naics_number, keywords_number,founder_count, president_count, ceo_presence_flag, naics_presence_flag ) ,(HV_count + MV_count) as total_count , 
	ROW_NUMBER() OVER (PARTITION BY t1.company_id ORDER BY t1.revenue DESC) AS RN
	FROM daas.dax_12136_temp_20250728 T1
	where(HV_count + MV_count) > employee_count
	AND company_status IN ('whitelist')
) 
as subquerry --WHERE RN = 1 
;

--
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- COMPANY 39 QA 

SELECT DISTINCT * FROM
(
	SELECT DISTINCT * EXCEPT(naics_number, keywords_number,founder_count, president_count, ceo_presence_flag, naics_presence_flag ) ,(HV_count + MV_count) as total_count, 
	ROW_NUMBER()OVER(PARTITION BY T1.company_id ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN  
	FROM daas.dax_12136_temp_20250728 T1 
	where(HV_count + MV_count) > employee_count
	AND company_status IN ('whitelist')
)
 as subquerry
; 
