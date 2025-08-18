--
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- COMPANY 38 Ingestion

with cte as
(
	SELECT DISTINCT * EXCEPT(naics_number, keywords_number, founder_count, president_count, ceo_presence_flag, naics_presence_flag), 
	ROW_NUMBER() OVER (PARTITION BY t1.company_id ORDER BY t1.revenue DESC) AS RN
	from daas.dax_12136_temp_20250728 T1 
	JOIN 
	(
			SELECT DISTINCT T1.company_id
			FROM daas.o_cds_tags AS T1 
			JOIN daas.dax_12136_temp_20250728 AS T2 ON T1.company_id = T2.company_id AND T1.tags = 'forbes_global_2000'
			WHERE T2.company_id NOT IN
			( 
				SELECT DISTINCT T1.company_id 
				FROM daas.person_top_positions_20250616_fixed_gr T1 
				JOIN daas.o_cds_tags T2 ON T1.company_id = T2.company_id AND T2.tags = 'forbes_global_2000'
				WHERE T1.title_level = 'chf' AND T1.title_level NOT IN ('brd', 'vp', 'dir', 'mgr', 'stf', 'other')
			)
	) T2 ON T1.company_id = T2.company_id
)
SeLECT DISTINCT primary_domain as domain , company_country AS countryCode, primary_name AS name, company_status AS status from cte 
WHERE rn = 1
; 

--
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- company - 38 (QA)

with cte as
(
	SELECT DISTINCT * EXCEPT(naics_number, keywords_number, founder_count, president_count, ceo_presence_flag, naics_presence_flag), 
	ROW_NUMBER() OVER (PARTITION BY t1.company_id ORDER BY t1.revenue DESC) AS RN
	from daas.dax_12136_temp_20250728 T1 
	JOIN
	(
			SELECT DISTINCT T1.company_id
			FROM daas.o_cds_tags AS T1 
			JOIN daas.dax_12136_temp_20250728 AS T2 ON T1.company_id = T2.company_id AND T1.tags = 'forbes_global_2000'
			WHERE T2.company_id NOT IN 
			( 
				SELECT DISTINCT T1.company_id 
				FROM daas.person_top_positions_20250616_fixed_gr T1 
				JOIN daas.o_cds_tags T2 ON T1.company_id = T2.company_id AND T2.tags = 'forbes_global_2000'
				WHERE T1.title_level = 'chf' AND T1.title_level NOT IN ('brd', 'vp', 'dir', 'mgr', 'stf', 'other')
			)
	) T2 ON T1.company_id = T2.company_id 
) 
SeLECT DISTINCT * EXCEPT(RN) from cte 
WHERE rn = 1
;  

--
