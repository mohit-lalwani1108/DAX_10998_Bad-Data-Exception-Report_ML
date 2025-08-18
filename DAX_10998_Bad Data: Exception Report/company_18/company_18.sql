--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- Company - 18

SELECT DISTINCT T2.primary_domain
FROM daas.o_cds_tags T1
JOIN daas.dax_12136_temp_20250728 T2
ON T1.company_id = T2.company_id
WHERE T1.tags = 'fortune_500'
AND company_country = 'US' AND company_status = 'whitelist'
;

--
