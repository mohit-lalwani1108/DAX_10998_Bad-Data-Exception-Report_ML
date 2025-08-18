--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- company-6 (ingestion)
 
SELECT DISTINCT primary_domain, company_country AS countryCode, primary_name AS name, company_status as status, employee_count, ROUND(NULLIF(revenue, '0')::DOUBLE*1E6) as revenue
from
(
	SELECT DISTINCT * EXCEPT(naics_number, keywords_number, ceo_count,founder_count, president_count, ceo_presence_flag, naics_presence_flag ) , 
	ROW_NUMBER()OVER(PARTITION BY company_id ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN 
	from daas.dax_12136_temp_20250728 
	WHERE employee_count >= 1000 AND  ROUND(NULLIF(revenue, '0')::DOUBLE*1E6) <= 10000000
	AND NOT 
		( 
		  LEFT(NULLIF(naics,''),2) IN ('91', '92', '99') 
		  OR
		  LEFT(NULLIF(naics,''),4) IN ('6112', '6111', '6113', '6114', '6115', '6116')
		  OR LOWER(TRIM(industry)) IN 
		  ( 
			'elementary and secondary schools', 'junior colleges', 'colleges', 'universities, and professional schools', 'business schools and computer and management training', 
			'technical and trade schools' , 'other schools and instruction industries'
		  ) 
	    ) 
) as subquerry WHERE rn=1  
; 

--
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
---- company-6 (qa) 
--
SELECT * EXCEPT(RN) from
( 
	SELECT DISTINCT * EXCEPT(naics_number, keywords_number, ceo_count,founder_count, president_count, ceo_presence_flag, naics_presence_flag ) , 
	ROW_NUMBER()OVER(PARTITION BY company_id ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN 
	from daas.dax_12136_temp_20250728 
	WHERE employee_count >= 1000 AND  ROUND(NULLIF(revenue, '0')::DOUBLE*1E6) <= 10000000
	AND NOT 
		( 
		  LEFT(NULLIF(naics,''),2) IN ('91', '92', '99') 
		  OR
		  LEFT(NULLIF(naics,''),4) IN ('6112', '6111', '6113', '6114', '6115', '6116')
		  OR LOWER(TRIM(industry)) IN 
		  ( 
			'elementary and secondary schools', 'junior colleges', 'colleges', 'universities, and professional schools', 'business schools and computer and management training', 
			'technical and trade schools' , 'other schools and instruction industries'
		  ) 
	    ) 
) as subquerry WHERE rn=1 
; 

--
