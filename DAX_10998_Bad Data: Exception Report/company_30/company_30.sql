-- 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- company-30   (ingestion)

SELECT DISTINCT primary_domain as domain, primary_name as name, company_country AS countryCode ,company_status AS status 
from
( 
    SELECT DISTINCT T1.* EXCEPT(naics_number, keywords_number, founder_count, president_count, ceo_presence_flag, naics_presence_flag),
    ROW_NUMBER()OVER(PARTITION BY T1.company_id ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN ,T2.domain AS alt_domain, T2.domain_type  
    FROM daas.dax_12136_temp_20250728 T1 
 	   JOIN daas.o_cds_alternate_domains T2 ON T1.company_id = T2.company_id 
    WHERE
    ( 
    REGEXP_LIKE(primary_domain, 'careers?|about|jobs') 
--	    AND NOT REGEXP_LIKE(LOWER(primary_name), '\\b(careers?|about|jobs)\\b')
    AND primary_name NOT ILIKE '%career%' AND primary_name NOT ILIKE '%about%' AND primary_name NOT ILIKE '%job%' AND primary_domain NOT ILIKE '%boutique%'
    )
    AND primary_domain <> primary_name 
    AND LOWER(TRIM(SPLIT_PART(primary_domain,'.',1))) <> REGEXP_REPLACE(LOWER(TRIM(primary_name)), '[^a-z0-9-]', '') 
) as subquerry WHERE RN = 1 
;

-- 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- company-30  (qa)

SELECT * EXCEPT(RN) FROM
(
    SELECT DISTINCT T1.* EXCEPT(naics_number, keywords_number, founder_count, president_count, ceo_presence_flag, naics_presence_flag),
    ROW_NUMBER()OVER(PARTITION BY T1.company_id ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN  
    FROM daas.dax_12136_temp_20250728 T1 
 	   JOIN daas.o_cds_alternate_domains T2 ON T1.company_id = T2.company_id 
    WHERE
    ( 
    REGEXP_LIKE(primary_domain, 'careers?|about|jobs') 
--	    AND NOT REGEXP_LIKE(LOWER(primary_name), '\\b(careers?|about|jobs)\\b')
    AND primary_name NOT ILIKE '%career%' AND primary_name NOT ILIKE '%about%' AND primary_name NOT ILIKE '%job%' AND primary_domain NOT ILIKE '%boutique%'
    ) 
    AND primary_domain <> primary_name 
    AND LOWER(TRIM(SPLIT_PART(primary_domain,'.',1))) <> REGEXP_REPLACE(LOWER(TRIM(primary_name)), '[^a-z0-9-]', '') 
--    AND NOT REGEXP_LIKE(LOWER(primary_name), REGEXP_REPLACE(primary_domain, '\\.', '')) 
) as subquerry WHERE RN = 1 
;  

-- 
