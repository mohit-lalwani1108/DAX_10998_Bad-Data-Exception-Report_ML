-- 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- company-10 (ingestion) 

SELECT DISTINCT primary_domain, company_country AS countryCode, primary_name AS name, company_status as status, naics as removeNaics from daas.dax_12136_temp_20250728 where
company_id IN
( 
	SELECT DISTINCT company_id FROM 
	( 
		SELECT company_id , EXPLODE(SPLIT(naics , '\\|')) AS naics 
		FROM daas.dax_12136_temp_20250728 
	) WHERE NULLIF(naics, '') IS NOT NULL AND
	NOT
	(
	(LEFT(naics::string, 2) IN (SELECT DISTINCT naics::string from daas.master_naics_code_desc_05242024 WHERE LENGTH(naics::string)=2 ) )
	OR 
	(LEFT(naics::string, 3)  IN (SELECT DISTINCT naics::string from daas.master_naics_code_desc_05242024 WHERE LENGTH(naics::string)=3 ))
	OR 
	( LEFT(naics::string, 4)  IN (SELECT DISTINCT naics::string from daas.master_naics_code_desc_05242024 WHERE LENGTH(naics::string)=4 ))
	OR 
	(LEFT(naics::string, 6)  IN (SELECT DISTINCT naics::string from daas.master_naics_code_desc_05242024 WHERE LENGTH(naics::string)=6 ))
	) 
)
;

-- 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- company-10   (qa) 

SELECT * EXCEPT(naics_number, keywords_number, ceo_count,founder_count, president_count, ceo_presence_flag, naics_presence_flag ) from daas.dax_12136_temp_20250728 where
company_id IN
(	
	SELECT DISTINCT company_id FROM 
	( 
		SELECT company_id , EXPLODE(SPLIT(naics , '\\|')) AS naics 
		FROM daas.dax_12136_temp_20250728 
	) WHERE NULLIF(naics, '') IS NOT NULL AND
	NOT
	(
	(LEFT(naics::string, 2) IN (SELECT DISTINCT naics::string from daas.master_naics_code_desc_05242024 WHERE LENGTH(naics::string)=2 ) )
	OR 
	(LEFT(naics::string, 3)  IN (SELECT DISTINCT naics::string from daas.master_naics_code_desc_05242024 WHERE LENGTH(naics::string)=3 ))
	OR 
	( LEFT(naics::string, 4)  IN (SELECT DISTINCT naics::string from daas.master_naics_code_desc_05242024 WHERE LENGTH(naics::string)=4 ))
	OR 
	(LEFT(naics::string, 6)  IN (SELECT DISTINCT naics::string from daas.master_naics_code_desc_05242024 WHERE LENGTH(naics::string)=6 ))
	)  
) 
; 
 
-- 
