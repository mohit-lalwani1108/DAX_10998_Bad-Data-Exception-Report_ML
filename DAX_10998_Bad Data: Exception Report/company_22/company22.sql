-- 
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------  
-- company-22  (ingestion)

SELECT DISTINCT primary_domain AS domain , company_country AS countryCode, primary_name AS name, company_status as status, ARRAY_JOIN(ARRAY_SORT(COLLECT_SET(digits)), '|') as removePhone  from 
( 
	SELECT DISTINCT * EXCEPT(naics_number, keywords_number, ceo_count,founder_count, president_count, ceo_presence_flag, naics_presence_flag ) , length(REGEXP_REPLACE(digits, '[^0-9x+]', '') ),
	ROW_NUMBER()OVER(PARTITION BY company_id ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN  
	from daas.dax_12136_temp_20250728
	WHERE (digits not ILIKE '%x%' and length(REGEXP_REPLACE(digits, '[^0-9x+]', '') ) > 15)
	OR (digits not ILIKE '%x%' and length(REGEXP_REPLACE(digits, '[^0-9x+]', '') ) < 7)
	OR  (( NOT REGEXP_LIKE(digits, '^[0-9]' ) OR ( digits ILIKE '%x%' AND LENGTH( SPLIT_PART(digits,'x',2) ) > 7 ) ) AND (( digits ILIKE '+1%' ) or ( digits not ILIKE '+%' )))
) as subquerry
WHERE rn=1 
AND  NOT( LENGTH (REGEXP_REPLACE(digits,'[^a-zA-Z0-9]','')) = 10 AND company_country = 'US' )
GROUP BY ALL 
;

-- 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- company-22  (qa) 
 
SELECT * EXCEPT(RN)
from
( 
	SELECT DISTINCT * EXCEPT(naics_number, keywords_number, ceo_count,founder_count, president_count, ceo_presence_flag, naics_presence_flag ) , length(REGEXP_REPLACE(digits, '[^0-9x+]', '') ),
	ROW_NUMBER()OVER(PARTITION BY company_id ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN  
	from daas.dax_12136_temp_20250728
	WHERE (digits not ILIKE '%x%' and length(REGEXP_REPLACE(digits, '[^0-9x+]', '') ) > 15)
	OR (digits not ILIKE '%x%' and length(REGEXP_REPLACE(digits, '[^0-9x+]', '') ) < 7)
	OR  (( NOT REGEXP_LIKE(digits, '^[0-9]' ) OR ( digits ILIKE '%x%' AND LENGTH( SPLIT_PART(digits,'x',2) ) > 7 ) ) AND (( digits ILIKE '+1%' ) or ( digits not ILIKE '+%' )))
) as subquerry WHERE rn=1
AND  NOT( LENGTH (REGEXP_REPLACE(digits,'[^a-zA-Z0-9]','')) = 10 AND company_country = 'US' )
;

-- 
