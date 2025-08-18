--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- company-28   (QA)

CREATE OR REPLACE TABLE daas.12136_TEMP AS
SELECT
DISTINCT T2.primary_domain, T2.primary_name, t2.company_country ,T2.company_status, T1.title 
FROM daas.t0export_20250703 T1
JOIN daas.dax_12136_temp_20250728 T2
ON LOWER(TRIM(t1.`domain`)) = LOWER(TRIM(t2.primary_domain))
WHERE
(
	T1.title ILIKE '%ceo%' 
	OR  T1.title ILIKE '%Chief executive officer%'
	OR REGEXP_LIKE(T1.title  , '(?i)(^|[^a-z0-9])c ?[-_,.\/&]? ?e ?[-_,.\/&]? ?o([^a-z0-9]|$)') 
	or T1.title ILIKE '%chief%executive%officer%' 
)
AND T2.company_status = 'whitelist'
;
 
--
------------------------------------------------------------------------------------------------------------------------------------------------------------
--

SELECT DISTINCT primary_domain as domain, company_country as countryCode , primary_name as name, company_status as status
from daas.dax_12136_temp_20250728
where LOWER(TRIM(primary_domain)) NOT IN ( SELECT DISTINCT LOWER(TRIM(primary_domain)) from daas.12136_TEMP )
AND company_status = 'whitelist'
;

--
