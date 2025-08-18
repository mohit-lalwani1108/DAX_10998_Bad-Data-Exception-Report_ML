-- Company 9 (Ingestion)
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--

SELECT DISTINCT primary_domain AS domain, company_country AS countryCode, primary_name AS name, company_status as status, naics from 
(  
	SELECT DISTINCT * EXCEPT(naics_number, keywords_number, ceo_count,founder_count, president_count, ceo_presence_flag, naics_presence_flag ) , 
	ROW_NUMBER()OVER(PARTITION BY company_id ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN 
	FROM daas.dax_12136_temp_20250728  
--	WHERE naics_number = 1 
	WHERE REGEXP_COUNT(TRIM(naics::string), '\\|') < 1
	AND NULLIF(naics,'') IS NOT NULL
	AND NOT
	(
	(LEFT(naics::string, 2) IN (SELECT DISTINCT naics::string from daas.master_naics_code_desc_05242024 WHERE LENGTH(naics::string)=2 ) )
	OR 
	(LEFT(naics::string, 3)  IN (SELECT DISTINCT naics::string from daas.master_naics_code_desc_05242024 WHERE LENGTH(naics::string)=3 ))
	OR 
	( LEFT(naics::string, 4)  IN (SELECT DISTINCT naics::string from daas.master_naics_code_desc_05242024 WHERE LENGTH(naics::string)=4 ))
	OR 
	(LEFT(naics::string, 6)  IN (SELECT DISTINCT naics::string from daas.master_naics_code_desc_05242024 WHERE LENGTH(naics::string)=6 ))
	)
) as subquerry WHERE rn=1 
;  
  
-- 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
  -- company-9   (QA) 

SELECT DISTINCT * EXCEPT(RN) from
(  
	SELECT DISTINCT * EXCEPT(naics_number, keywords_number, ceo_count,founder_count, president_count, ceo_presence_flag, naics_presence_flag ) , 
	ROW_NUMBER()OVER(PARTITION BY company_id ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN 
	FROM daas.dax_12136_temp_20250728  
--	WHERE naics_number = 1 
	WHERE REGEXP_COUNT(TRIM(naics::string), '\\|') < 1
	AND NULLIF(naics,'') IS NOT NULL
	AND NOT
	(
	(LEFT(naics::string, 2) IN (SELECT DISTINCT naics::string from daas.master_naics_code_desc_05242024 WHERE LENGTH(naics::string)=2 ) )
	OR 
	(LEFT(naics::string, 3)  IN (SELECT DISTINCT naics::string from daas.master_naics_code_desc_05242024 WHERE LENGTH(naics::string)=3 ))
	OR 
	( LEFT(naics::string, 4)  IN (SELECT DISTINCT naics::string from daas.master_naics_code_desc_05242024 WHERE LENGTH(naics::string)=4 ))
	OR 
	(LEFT(naics::string, 6)  IN (SELECT DISTINCT naics::string from daas.master_naics_code_desc_05242024 WHERE LENGTH(naics::string)=6 ))
	)
) as subquerry WHERE rn=1 
; 

-- 
