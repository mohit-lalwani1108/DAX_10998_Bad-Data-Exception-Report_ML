--
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- COmpany - 40 (QA)

SELECT DISTINCT primary_domain, company_country AS countryCode, primary_name AS name, company_status as status, ds_it, ds_marketing, ds_operations, ds_rnd, ds_sales, ds_finance
ds_hr, ds_legal, ds_procurement 
, employee_count
FROM
( 
	SELECT DISTINCT T1.* ,T2.* , 
	ROW_NUMBER()OVER(PARTITION BY T1.company_id ORDER BY CASE WHEN LENGTH(digits) = 10 THEN 0 ELSE 1 END, LENGTH(digits) DESC) AS RN 
	FROM daas.dax_12136_temp_20250728 T1
	JOIN daas.o_cds_department_size T2 ON T1.company_id = T2.company_id 
	WHERE
	( 
	ds_it > (T1.employee_count) OR ds_marketing > (T1.employee_count) OR ds_operations > (T1.employee_count) OR ds_rnd > (T1.employee_count) OR ds_sales > (T1.employee_count) OR 
	ds_finance > (T1.employee_count) OR ds_hr > (T1.employee_count) OR ds_legal > (T1.employee_count) OR ds_procurement > (T1.employee_count) 
	)   
	and (ds_it + ds_marketing + ds_operations + ds_rnd + ds_sales + ds_finance + ds_hr + ds_legal + ds_procurement) is not null 
) AS SUBQUERRY WHERE RN = 1 
;


