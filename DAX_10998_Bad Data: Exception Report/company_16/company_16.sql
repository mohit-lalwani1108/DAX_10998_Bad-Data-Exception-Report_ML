-- 
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------  
-- company-16  (ingestion)

SELECT DISTINCT primary_domain AS domain , company_country AS countryCode, primary_name AS name, company_status as status, employee_count 
FROM
(
	SELECT DISTINCT T1.* ,  CEILING((ds_it + ds_marketing + ds_operations + ds_rnd + ds_sales + ds_finance + ds_hr + ds_legal + ds_procurement)) AS dept_size, 
	ROW_NUMBER()OVER(PARTITION BY T1.company_id ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN 
	FROM daas.dax_12136_temp_20250728 T1 
	JOIN daas.o_cds_department_size T2 ON T1.company_id = T2.company_id 
	WHERE CEILING((ds_it + ds_marketing + ds_operations + ds_rnd + ds_sales + ds_finance + ds_hr + ds_legal + ds_procurement)) > (T1.employee_count*1.2)  
	AND T1.company_status = 'whitelist'
) AS SUBQUERRY WHERE RN = 1 
;

-- 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- company-16   (qa)

SELECT DISTINCT * EXCEPT(RN)
FROM 
(
	SELECT DISTINCT T1.* ,  CEILING((ds_it + ds_marketing + ds_operations + ds_rnd + ds_sales + ds_finance + ds_hr + ds_legal + ds_procurement)) AS dept_size, 
	ROW_NUMBER()OVER(PARTITION BY T1.company_id ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN 
	FROM daas.dax_12136_temp_20250728 T1 
	JOIN daas.o_cds_department_size T2 ON T1.company_id = T2.company_id 
	WHERE CEILING((ds_it + ds_marketing + ds_operations + ds_rnd + ds_sales + ds_finance + ds_hr + ds_legal + ds_procurement)) > ( T1.employee_count*1.2) 
	AND T1.company_status = 'whitelist' 
) AS SUBQUERRY WHERE RN = 1  
;

--
