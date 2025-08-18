--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- company-29   (ingestion)  

SELECT DISTINCT primary_domain as domain, company_country AS countryCode, primary_name AS name, company_status as status , Street1 as street , city, state, postal_code as zipcode
from
( 
	SELECT DISTINCT T1.* EXCEPT(naics_number, keywords_number,founder_count, president_count, ceo_presence_flag, naics_presence_flag ) , 
	ROW_NUMBER()OVER(PARTITION BY company_id ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN,LEFT(T1.postal_code, 5) lp ,zip 
	FROM daas.dax_12136_temp_20250728 T1 	
	JOIN daas.TEMP_table_city_state_ml T2 ON LEFT(T1.postal_code, 5) = zip
	WHERE lower(trim(T1.state)) != lower(trim(T2.state_id)) AND T2.zip IS NOT NULL AND T1.company_country = 'US'
) as subquerry WHERE RN = 1
;

-- 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- company-29  (qa)    	
 
SELECT DISTINCT * EXCEPT(RN) FROM
(
	SELECT DISTINCT T1.* EXCEPT(naics_number, keywords_number,founder_count, president_count, ceo_presence_flag, naics_presence_flag ) , 
	ROW_NUMBER()OVER(PARTITION BY company_id ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN ,
	T2.city , T2.state_id , T2.state_name,  T2.zip 
	FROM daas.dax_12136_temp_20250728 T1 	
	JOIN daas.TEMP_table_city_state_ml T2 ON LEFT(T1.postal_code, 5) = zip
	WHERE T1.state != T2.state_id AND T2.zip IS NOT NULL AND T1.company_country = 'US'
) as subquerry WHERE RN = 1 
;

-- 
