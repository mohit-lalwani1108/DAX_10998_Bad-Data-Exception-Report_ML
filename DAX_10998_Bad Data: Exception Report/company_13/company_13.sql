-- 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- company-13    (QA) 

WITH CTE AS
(
	SELECT *  
	FROM
	(
		SELECT DISTINCT T2.company_id, T2.primary_domain, T2.primary_name, t2.company_country ,T2.company_status, T1.*
		FROM daas.t0export_20250703 T1 
		JOIN daas.dax_12136_temp_20250728 T2 
		ON t1.`domain` = t2.primary_domain and COALESCE(t1.primaryCountry,t1.personalCountry,'US') = T2.company_country 
		WHERE 
		REGEXP_LIKE(LOWER(TRIM(T1.title)), '^c[ .\\-/\\_]?e[ .\\-/\\_]?o$') 
		OR
		LOWER(TRIM(T1.title)) = 'chief executive officer' 
		ORDER BY T2.primary_domain 
	) AS SUBQUERRY
)
, cte2 as
(
	SELECT *, count(*) over(partition by primary_domain) as cn from 
	( 
		SELECT DISTINCT t1.*  FROM CTE t1 
		join daas.dax_12136_temp_20250728 t2 on t1.company_id = t2.company_id 
		order by primary_domain 
	) 
) SELECT DISTINCT company_id, primary_domain, primary_name, company_country, company_status, contactId, firstName, lastName, email, personalMobile, workPhones, domain, primarycountry, companyname, title, lvl, dpt,personalCity, personalState, personalZipCode, personalCountry,linkedinLocation , linkedinUrl, alternateLinkedinUrl from cte2 where cn > 1
;

--
