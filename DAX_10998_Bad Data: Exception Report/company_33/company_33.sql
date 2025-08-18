--
------------------------,--------------------------------------------------------------------------------------------------------------------------------------------------
-- COMPANY 33 Ingestion 

SELECT DISTINCT primary_domain AS domain, company_country AS countryCode, primary_name AS name, company_status as status, naics
from
(
	SELECT DISTINCT * EXCEPT(naics_number, keywords_number,founder_count, president_count, ceo_presence_flag, naics_presence_flag ) , 
	ROW_NUMBER()OVER(PARTITION BY company_id ORDER BY null) AS RN 
	FROM daas.dax_12136_temp_20250728 T1 
	where (naics is null or naics = '')
	and company_status = 'whitelist'
) as subquerry WHERE RN = 1 
;

--
------------------------,--------------------------------------------------------------------------------------------------------------------------------------------------
-- Company 33 QA

SELECT * EXCEPT(RN) FROM 
(
	SELECT DISTINCT * EXCEPT(naics_number, keywords_number,founder_count, president_count, ceo_presence_flag, naics_presence_flag ) , 
	ROW_NUMBER()OVER(PARTITION BY company_id ORDER BY null) AS RN 
	FROM daas.dax_12136_temp_20250728 T1 
	where (naics is null or naics = '')
	and company_status = 'whitelist'
) as subquerry WHERE RN = 1 
; 

--
