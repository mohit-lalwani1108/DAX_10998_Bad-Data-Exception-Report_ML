--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- Company-17

SELECT DISTINCT T1.primary_domain 
from daas.o_cds T1 
JOIN
(
	SELECT DISTINCT T2.primary_domain , T1.company_id , T2.company_country 
	FROM daas.o_cds_tags T1 
	JOIN daas.dax_12136_temp_20250728 T2 
	ON T1.company_id = T2.company_id 
	WHERE T1.tags = 'fortune_1000' and company_country = 'US' AND company_status = 'whitelist'
) AS T2 ON T1.company_id = T2.company_id AND T1.company_country = T2.company_country 
where T1.is_hq = TRUE
; 

--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
