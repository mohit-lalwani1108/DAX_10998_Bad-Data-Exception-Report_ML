--
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------  
-- company-19  (ingestion) 

SELECT DISTINCT primary_domain as domain , company_country AS countryCode, primary_name AS name, company_status as status, street1 as street , city, state, postal_code as zipcode from 
(
	SELECT DISTINCT * EXCEPT(naics_number, keywords_number, ceo_count,founder_count, president_count, ceo_presence_flag, naics_presence_flag ) , 
	ROW_NUMBER()OVER(PARTITION BY company_id ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN  
	from daas.dax_12136_temp_20250728 
	WHERE company_country = 'US' AND 
	NULLIF(STATE,'') NOT IN ('WA', 'AL', 'AK', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MS', 'MO', 'MT', 'FL', 'NC', 'ND', 'NE', 'NM', 'NH', 'NV', 'NY', 'OH', 'OK', 'OR', 'RI', 'SC', 'MN', 'NJ', 'PA', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WI', 'WV', 'WY', 'PR')
) as subquerrY 
WHERE rn=1
;

-- 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- company-19   (qa) 

SELECT DISTINCT * EXCEPT(RN) from 
( 
	SELECT DISTINCT * EXCEPT(naics_number, keywords_number, ceo_count,founder_count, president_count, ceo_presence_flag, naics_presence_flag ) , 
	ROW_NUMBER()OVER(PARTITION BY company_id ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN  
	from daas.dax_12136_temp_20250728 
	WHERE company_country = 'US' AND 
	NULLIF(STATE,'') NOT IN ('WA', 'AL', 'AK', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MS', 'MO', 'MT', 'FL', 'NC', 'ND', 'NE', 'NM', 'NH', 'NV', 'NY', 'OH', 'OK', 'OR', 'RI', 'SC', 'MN', 'NJ', 'PA', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WI', 'WV', 'WY', 'PR')
) as subquerry WHERE rn=1   
;  
 
-- 
