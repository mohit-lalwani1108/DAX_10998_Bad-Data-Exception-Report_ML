--
------------------------,--------------------------------------------------------------------------------------------------------------------------------------------------
-- Company 34 Ingestion 

SELECT DISTINCT primary_domain AS domain
, company_country AS countryCode, primary_name AS name, company_status as status, employee_count, tags FROM
(
	SELECT DISTINCT T1.* EXCEPT(naics_number, keywords_number, founder_count, president_count, ceo_presence_flag, naics_presence_flag), T2.tags
--	ROW_NUMBER()OVER(PARTITION BY T1.company_id ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN 
	FROM daas.dax_12136_temp_20250728 AS T1 
	JOIN daas.o_cds_tags AS T2 ON T1.company_id =T2.company_id 
	WHERE LOWER(TRIM(T2.tags)) NOT IN 
	( 
	'100_fastest_growing', 
	'fast_500_deloitte',
	'federal_contractors',
	'forbes_global_2000',
	'fortune_500',
	'fortune_1000',
	'funding_round_a',
	'funding_round_b',
	'funding_round_c', 
	'funding_round_d',
	'funding_round_seed',
	'inc_5000', 
	'linkedin_top_startups_2021',
	'managed_service_providers',
	'public_companies',
	'saas_1000',
	'value_added_resellers'
	)
) as subquerry
--WHERE rn = 1 
;

--
------------------------,--------------------------------------------------------------------------------------------------------------------------------------------------
-- COMPANY 34 QA

SELECT DISTINCT * --EXCEPT(RN)
FROM 
(
	SELECT DISTINCT T1.* EXCEPT(naics_number, keywords_number, founder_count, president_count, ceo_presence_flag, naics_presence_flag), T2.tags, 
	ROW_NUMBER()OVER(PARTITION BY T1.company_id||T2.tags ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN 
	FROM daas.dax_12136_temp_20250728 AS T1 
	JOIN daas.o_cds_tags AS T2 ON T1.company_id =T2.company_id 
	WHERE LOWER(TRIM(T2.tags)) NOT IN
	( 
	'100_fastest_growing',
	'fast_500_deloitte',
	'federal_contractors',
	'forbes_global_2000',
	'fortune_500',
	'fortune_1000',
	'funding_round_a',
	'funding_round_b',
	'funding_round_c', 
	'funding_round_d',
	'funding_round_seed',
	'inc_5000', 
	'linkedin_top_startups_2021',
	'managed_service_providers',
	'public_companies',
	'saas_1000',
	'value_added_resellers'
	)
) AS SUBQUERRY 
WHERE rn = 1
;

--
