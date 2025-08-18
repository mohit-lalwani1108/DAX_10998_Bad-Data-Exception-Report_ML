--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Taking the backup of previous table ( from 42-46 we need to send only QA files )

-- For RUle 42-46

DESCRIBE HISTORY daas.o_cds ;

--
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Take the backup of the week in which last bad data was delivered ( basically we gonna compare the results of that cds with the latest cds table )

CREATE OR REPLACE TABLE daas.DAX_12136_backup_cds_20250729_ML AS
SELECT * FROM daas.o_cds VERSION AS OF 75 ;

--
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--

CREATE OR REPLACE TABLE si_dataops_prod.daas.DAX_12136_backup_cds_20250729_ML  AS 
SELECT DISTINCT *
FROM daas.DAX_12136_backup_cds_20250729_ML
WHERE company_status = 'whitelist'
;

--
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--

CREATE TABLE si_dataops_prod.daas.DAX_12136_comparison_results_ML AS -- create new with companies that changed their data more than 10%
WITH prep AS
	(
		SELECT DISTINCT  
			 oc.company_id 
			,oc.primary_name
			,oc.primary_domain
			,oc.company_country
			,oc.linkedin_url
			,bcd.employee_count AS previous_employee_count
			,oc.employee_count AS current_employee_count
			,((oc.employee_count / bcd.employee_count) * 100) - 100 AS employee_count_diff
			,try_cast(bcd.revenue AS int) AS previous_revenue
			,try_cast(oc.revenue AS int) AS current_revenue
			,((try_cast(oc.revenue AS int) / try_cast(bcd.revenue AS int)) * 100) - 100 AS revenue_diff
			,COUNT(oc.company_id) OVER(PARTITION BY oc.primary_name, oc.location_id) AS dupe
		FROM 
			 si_dataops_prod.daas.o_cds oc 
		LEFT JOIN si_dataops_prod.daas.DAX_12136_backup_cds_20250729_ML bcd ON oc.company_id = bcd.company_id AND oc.location_id = bcd.location_id 
		WHERE 1=1
			AND bcd.employee_count != 0
			AND try_cast(bcd.revenue AS int) != 0
			AND oc.company_status = 'whitelist' 
			AND bcd.is_hq IS TRUE
			AND oc.is_hq IS TRUE
	)
SELECT * 
FROM 
	 prep 
WHERE 1=1 
	 AND 
	 	(
	 		employee_count_diff NOT BETWEEN -10 AND 10
	 	    OR revenue_diff NOT BETWEEN -10 AND 10
	 	)
;

--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
--

DROP TABLE si_dataops_prod.daas.DAX_12136_company_42_ML ;

--
------------------------------------------------------------------------------------------------------------------------------------------------------
--

SHOW TABLES IN DAAS LIKE '*bp15_company_file*' ;
 
-- 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- Company 42

CREATE TABLE si_dataops_prod.daas.DAX_12136_company_42_ML AS
WITH prep AS -- this part returns the result for companies that changed their revenue and\or employee count more that 10% from previous
( 
SELECT CASE
		WHEN website ILIKE 'http://www.%' THEN LOWER(SUBSTRING(website, 12))
		WHEN website ILIKE 'https://www.%' THEN LOWER(SUBSTRING(website, 13))
		WHEN website ILIKE 'http://%' THEN LOWER(SUBSTRING(website, 8))
		WHEN website ILIKE 'https://%' THEN LOWER(SUBSTRING(website, 9))
		ELSE NULL 
	   END AS cleaned_website, country_code, MAX(updated_month) AS max_month, MAX(social_followers :: int) AS max_follow
FROM si_dataops_prod.daas.bp15_company_file_jul2025_gr WHERE employees_num != '\N' AND social_followers != '\N'
GROUP BY 1, 2 
)
SELECT
	 cr.primary_name
	,cr.primary_domain
	,cr.company_country
	,cr.linkedin_url
	,cr.previous_employee_count
	,cr.current_employee_count
	,cr.employee_count_diff
	,cr.previous_employee_count - cr.current_employee_count AS employee_diff_amount
	,CASE WHEN (CASE 
		WHEN bp15.website ILIKE 'http://www.%' THEN LOWER(SUBSTRING(bp15.website, 12))
		WHEN bp15.website ILIKE 'https://www.%' THEN LOWER(SUBSTRING(bp15.website, 13))
		WHEN bp15.website ILIKE 'http://%' THEN LOWER(SUBSTRING(bp15.website, 8))
		WHEN bp15.website ILIKE 'https://%' THEN LOWER(SUBSTRING(bp15.website, 9))
		ELSE NULL 
	   END) IS NULL THEN 0 ELSE (bp15.employees_num :: int * 1.1) END AS BP_15_employee_count
	,cr.previous_revenue * 1000000 AS previous_revenue
	,cr.current_revenue * 1000000 AS current_revenue
	,cr.revenue_diff
	,(cr.previous_revenue - cr.current_revenue) * 1000000 AS revenue_diff_amount
FROM si_dataops_prod.daas.DAX_12136_comparison_results_ML cr
LEFT JOIN prep bp ON cr.primary_domain = bp.cleaned_website
LEFT JOIN si_dataops_prod.daas.bp15_company_file_jul2025_gr bp15 ON bp.cleaned_website = CASE 
		WHEN bp15.website ILIKE 'http://www.%' THEN LOWER(SUBSTRING(bp15.website, 12))
		WHEN bp15.website ILIKE 'https://www.%' THEN LOWER(SUBSTRING(bp15.website, 13))
		WHEN bp15.website ILIKE 'http://%' THEN LOWER(SUBSTRING(bp15.website, 8))
		WHEN bp15.website ILIKE 'https://%' THEN LOWER(SUBSTRING(bp15.website, 9))
		ELSE NULL 
	   END 
WHERE cr.company_country = UPPER(bp.country_code) AND cr.company_country = UPPER(bp15.country_code)
AND bp15.social_followers = bp.max_follow AND bp15.social_followers != '\N' AND bp15.employees_num != '\N'
; 

-- 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- Data delivery for company-42

SELECT DISTINCT * FROM si_dataops_prod.daas.DAX_12136_company_42_ML;

--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
--Company 43

DROP TABLE si_dataops_prod.daas.DAX_12136_Company_43_ML ;

-- 
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE TABLE si_dataops_prod.daas.DAX_12136_Company_43_ML AS
WITH prep AS 
(  
SELECT  
	 oc.company_id
	,oc.primary_domain 
	,oc.primary_name 
	,oc.street1 
	,oc.city
	,oc.state
	,oc.postal_code
	,oc.company_country
	,oc.digits
	,oc.primary_naics_code
	,n.industry
	,oc.linkedin_url
	,oc.employee_count
	,oc.revenue
	,oc.company_status
	,COUNT(DISTINCT CASE WHEN ptp.person_source = 'research' THEN ptp.person_id ELSE NULL END) AS HV_count
	,COUNT(DISTINCT CASE WHEN ptp.person_source = 'siq' THEN ptp.person_id ELSE NULL END) AS MV_count
FROM
	 si_dataops_prod.daas.o_cds oc
LEFT JOIN
	 si_dataops_prod.daas.master_naics_code_desc_05242024 n ON oc.primary_naics_code = n.naics
LEFT JOIN
	 daas.person_top_positions_20250616_fixed_gr ptp ON oc.company_id = ptp.company_id
WHERE 1=1
	 AND oc.employee_count > 600000
	 AND oc.company_status IN ('active', 'whitelist')
	 AND primary_domain NOT ILIKE 'walmart.%'
	 AND primary_domain NOT ILIKE 'Amazon.%'
	 AND primary_domain NOT ILIKE 'Defense.gov'
	 AND primary_domain NOT ILIKE 'Mcdonalds.%'
	 AND primary_domain NOT ILIKE 'Foxconn.%'
	 AND primary_domain NOT ILIKE 'Cnpc.%'
	 AND primary_domain NOT ILIKE 'Nhs.%'
	 AND primary_domain NOT ILIKE 'Mod.gov.in'
	 AND primary_domain NOT ILIKE 'Indianrail.gov.in'
	 AND primary_domain NOT ILIKE 'Chinamil.com.cn'
	 AND primary_domain NOT ILIKE 'Sgcc.com.cn'
	 AND primary_domain NOT ILIKE 'Education.gouv.fr'
	 AND primary_domain NOT ILIKE 'Tata.com'--!
	 AND primary_domain NOT ILIKE 'Usps.com'
	 AND primary_domain NOT ILIKE 'Sinopecgroup.com'
	 AND oc.primary_name NOT ILIKE 'amazon%'
	 AND oc.is_hq IS TRUE
	 AND oc.company_country = UPPER(oc.company_country)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
ORDER BY 2
)
SELECT DISTINCT
	 p.company_id
	,p.primary_domain 
	,p.primary_name
	,p.street1
	,p.city
	,p.state
	,p.postal_code
	,p.company_country
	,p.digits
	,p.primary_naics_code
	,p.industry
	,p.linkedin_url
	,p.employee_count
	,p.revenue
	,p.company_status 
	,p.HV_count
	,p.MV_count
FROM prep p 
;

--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- Data delivery for company-43

SELECT DISTINCT * FROM si_dataops_prod.daas.DAX_12136_Company_43_ML ;

-- 
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Company 44  

DROP TABLE si_dataops_prod.daas.DAX_12136_Company_44_ML ;

-- 
 
CREATE TABLE si_dataops_prod.daas.DAX_12136_Company_44_ML AS 
SELECT DISTINCT oc.*
FROM si_dataops_prod.daas.o_cds oc 
WHERE oc.company_status = 'inactive' 
AND oc.company_id IN (SELECT DISTINCT company_id FROM si_dataops_prod.daas.DAX_12136_backup_cds_20250729_ML) 
;  

-- 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- Data delivery for company-44

SELECT DISTINCT * FROM si_dataops_prod.daas.DAX_12136_Company_44_ML  ;

-- 
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Company 45

DROP TABLE si_dataops_prod.daas.DAX_12136_Company_45_ML ;

--
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--

CREATE TABLE si_dataops_prod.daas.DAX_12136_Company_45_ML AS 
SELECT DISTINCT oc.* 
FROM si_dataops_prod.daas.o_cds oc 
WHERE oc.company_status = 'active'
AND oc.company_id IN (SELECT DISTINCT company_id FROM si_dataops_prod.daas.DAX_12136_backup_cds_20250729_ML) 
; 

--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- Data delivery for company-45

SELECT DISTINCT * FROM daas.DAX_12136_Company_45_ML ; 

--
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Company 46   

DROP TABLE si_dataops_prod.daas.DAX_12136_Company_46_ML ;

-- 
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--

CREATE TABLE si_dataops_prod.daas.DAX_12136_Company_46_ML AS 
SELECT DISTINCT
	 oc.primary_domain AS `domain` 
	,oc.company_country AS countryCode   
	,oc.location_id AS locationId 
	,bc.linkedin_url AS Old_LI_URL 
	,oc.linkedin_url AS Current_LI_URL
	,TRUE AS Status 
FROM
	 si_dataops_prod.daas.o_cds oc
INNER JOIN 
	 si_dataops_prod.daas.DAX_12136_backup_cds_20250729_ML bc ON oc.company_id = bc.company_id
WHERE
	 oc.company_status = 'whitelist'
	 AND REGEXP_REPLACE(oc.linkedin_url, '.*linkedin.com', '') != REGEXP_REPLACE(bc.linkedin_url, '.*linkedin.com', '')
; 

--
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Data for Company 46

SELECT DISTINCT * FROM daas.DAX_12136_Company_46_ML ;

--
