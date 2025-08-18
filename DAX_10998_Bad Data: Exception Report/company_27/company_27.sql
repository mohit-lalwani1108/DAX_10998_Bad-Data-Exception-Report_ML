-- 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- company-27   (ingestion) 

SELECT DISTINCT primary_domain as domain, company_country AS countryCode , last_funding_round as Round , last_funding_date as funding_Month ,last_funding_amount AS funding_Round_Amount
from
( 
	SELECT DISTINCT T1.* EXCEPT(naics_number, keywords_number, ceo_count,founder_count, president_count, ceo_presence_flag, naics_presence_flag ) , 
	ROW_NUMBER()OVER(PARTITION BY T1.company_id ORDER BY CASE WHEN LENGTH(T1.digits) = 10 THEN 0 ELSE 1 END, LENGTH(T1.digits) DESC) AS RN , ROUND(NULLIF(T2.actual_revenue, '0')::DOUBLE*1E6) as actual_revenue , 
	T2.last_funding_round , T2.last_funding_date , T2.last_funding_amount
	from daas.dax_12136_temp_20250728 T1 JOIN daas.o_cds T2 ON T1.company_id = T2.company_id  
	WHERE ROUND(NULLIF(T2.actual_revenue, '0')::DOUBLE*1E6)  > 5000000 AND T2.last_funding_round= 'Seed' 
) as subquerry WHERE RN = 1 
;

--                                                                                               
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- company-27  (qa) 

SELECT * EXCEPT(RN) FROM 
(
	SELECT DISTINCT T1.* EXCEPT(naics_number, keywords_number, ceo_count,founder_count, president_count, ceo_presence_flag, naics_presence_flag ) , 
	ROW_NUMBER()OVER(PARTITION BY T1.company_id ORDER BY CASE WHEN LENGTH(T1.digits) = 10 THEN 0 ELSE 1 END, LENGTH(T1.digits) DESC) AS RN , ROUND(NULLIF(T2.actual_revenue, '0')::DOUBLE*1E6) as actual_revenue , 
	T2.last_funding_round , T2.last_funding_date , T2.last_funding_amount
	from daas.dax_12136_temp_20250728 T1 JOIN daas.o_cds T2 ON T1.company_id = T2.company_id  
	WHERE ROUND(NULLIF(T2.actual_revenue, '0')::DOUBLE*1E6)  > 5000000 AND T2.last_funding_round= 'Seed' 
) as subquerry 
WHERE RN = 1
;

-- 
