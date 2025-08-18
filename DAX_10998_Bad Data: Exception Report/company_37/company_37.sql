--
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- COMPANY 37 Ingestion 

SELECT DISTINCT primary_domain as domain , company_country AS countryCode, primary_name AS name, company_status AS status, LENGTH(primary_name) as length , HV_count, MV_count , (MV_count+MV_count) as total_count 
from daas.dax_12136_temp_20250728 
WHERE primary_domain IN 
(
	SELECT primary_domain
	FROM daas.o_cds 
	WHERE company_status IN ('active','whitelist')
	ORDER BY LENGTH(primary_name) DESC  
) AND LENGTH(primary_name) > 60 
; 

--
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Company 37 QA 

SELECT * EXCEPT(naics_number, keywords_number,founder_count, president_count, ceo_presence_flag, naics_presence_flag, RN ) FROM 
(
	SELECT DISTINCT * , ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY revenue DESC) AS RN 
	from daas.dax_12136_temp_20250728 
	WHERE primary_domain IN
	(
		SELECT primary_domain
		FROM daas.o_cds 
		WHERE company_status IN ('active','whitelist')
		ORDER BY LENGTH(primary_name) DESC  
	) AND LENGTH(primary_name) > 60 
) AS subquerry 
where RN = 1 
; 
 
--
