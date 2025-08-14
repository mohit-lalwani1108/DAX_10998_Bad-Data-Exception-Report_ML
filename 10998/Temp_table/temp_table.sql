CREATE TABLE daas.dax_10998_temp_20250625
SELECT DISTINCT 
	 oc.company_id 
	,oc.primary_domain
	,oc.primary_name
	,oc.street1
	,oc.city
	,oc.state
	,oc.postal_code
	,oc.company_country
	,oc.digits
	,ARRAY_JOIN(ARRAY_SORT(COLLECT_SET(ocn.naics)), '|') AS naics
	,n.industry
	,oc.linkedin_url 
	,oc.employee_count
	,oc.revenue
	,oc.company_status
	,ARRAY_JOIN(ARRAY_SORT(COLLECT_SET(ocs.specialties)), '|') AS keywords
	,COUNT(DISTINCT CASE WHEN ptp.person_source = 'research' THEN ptp.person_id ELSE NULL END) AS HV_count
	,COUNT(DISTINCT CASE WHEN ptp.person_source = 'siq' THEN ptp.person_id ELSE NULL END) AS MV_count
	,COUNT(DISTINCT ocn.naics) AS naics_number
	,COUNT(DISTINCT ocs.specialties) AS keywords_number
	,COUNT(DISTINCT CASE WHEN ((REGEXP_LIKE(ptp.title  , '(?i)(^|[^a-z0-9])c ?[-_,.\/&]? ?e ?[-_,.\/&]? ?o([^a-z0-9]|$)') or ptp.title ILIKE '%chief%executive%officer%')) THEN 1 ELSE NULL END) AS ceo_count
	,COUNT(DISTINCT CASE WHEN ptp.title ILIKE '%founder%'  THEN 1 ELSE NULL END) AS founder_count
	,COUNT(DISTINCT CASE WHEN 
		REGEXP_LIKE(REGEXP_REPLACE(title, 'Vice.President|V.President|Vice...President|Vice..President|Vice and President|President, Vice|V\. President',''), '(^|[^a-z0-9])President([^a-z0-9]|$)')
		AND title NOT ILIKE '%vice%president%'  THEN 1 ELSE NULL END) AS president_count
	,MAX(CASE WHEN ((REGEXP_LIKE(ptp.title  , '(?i)(^|[^a-z0-9])c ?[-_,.\/&]? ?e ?[-_,.\/&]? ?o([^a-z0-9]|$)') or ptp.title ILIKE '%chief%executive%officer%')) THEN TRUE ELSE FALSE END) AS ceo_presence_flag
--	,TRUE::BOOLEAN AS ceo_presence_flag
	,CASE WHEN ocn.company_id IS NULL THEN TRUE ELSE FALSE END AS naics_presence_flag
--SELECT count(*) 
FROM 
	 si_dataops_prod.daas.o_cds oc
LEFT JOIN
	 si_dataops_prod.daas.master_naics_code_desc_05242024 n ON oc.primary_naics_code = n.naics
LEFT JOIN
	 si_dataops_prod.daas.person_top_positions_20250616_fixed_gr ptp ON oc.company_id = ptp.company_id
LEFT JOIN
	 daas.o_cds_naics ocn ON oc.company_id = ocn.company_id
LEFT JOIN 
	 daas.o_cds_specialties ocs ON oc.company_id = ocs.company_id
WHERE oc.company_status IN ('active', 'whitelist') 
GROUP BY ALL
--LIMIT 1000 
;
