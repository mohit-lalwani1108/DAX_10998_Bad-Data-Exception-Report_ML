-- 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- COMPANY- 36 Ingestion

SELECT DISTINCT primary_domain as domain , company_country AS countryCode, primary_name AS name, company_status AS status, linkedin_url as li_url FROM 
(SELECT DISTINCT t1.primary_domain, t1.company_country, t1.primary_name, t1.company_status, t1.revenue, t1.linkedin_url,
ROW_NUMBER() OVER (PARTITION BY t1.company_id ORDER BY t1.revenue DESC) AS RN 
FROM daas.dax_12136_temp_20250728 t1 
JOIN (SELECT company_id, linkedin_url,revenue, primary_domain  FROM daas.dax_12136_temp_20250728 WHERE  
company_country = 'US' AND linkedin_url IS NOT NULL AND company_status = 'whitelist' order by ROUND(NULLIF(revenue, '0')::DOUBLE * 1E6) DESC limit 20000  ) t2 
ON t1.linkedin_url = t2.linkedin_url 
WHERE t1.company_status IN ('active', 'whitelist')
AND t1.company_id <>  t2.company_id
AND t1.primary_domain <> T2.primary_domain
)
AS subquery WHERE RN = 1
;

-- 
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Company - 36 QA 

SELECT DISTINCT * EXCEPT(RN) FROM
(SELECT DISTINCT t1.primary_domain, t1.company_country, t1.primary_name, t1.company_status, t1.revenue, t1.linkedin_url,
ROW_NUMBER() OVER (PARTITION BY t1.company_id ORDER BY t1.revenue DESC) AS RN 
FROM daas.dax_12136_temp_20250728 t1 
JOIN (SELECT company_id, linkedin_url,revenue, primary_domain  FROM daas.dax_12136_temp_20250728 WHERE  
company_country = 'US' AND linkedin_url IS NOT NULL AND company_status = 'whitelist' order by ROUND(NULLIF(revenue, '0')::DOUBLE * 1E6) DESC limit 20000  ) t2 
ON t1.linkedin_url = t2.linkedin_url 
WHERE t1.company_status IN ('active', 'whitelist')
AND t1.company_id <>  t2.company_id
AND t1.primary_domain <> T2.primary_domain
)
AS subquery WHERE RN = 1 
;

--
