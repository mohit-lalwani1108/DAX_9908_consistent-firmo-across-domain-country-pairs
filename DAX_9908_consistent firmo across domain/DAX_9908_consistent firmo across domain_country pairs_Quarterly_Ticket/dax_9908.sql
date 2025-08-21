--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
--

CREATE TABLE daas.dax_11834_Master_Domains_20250723_ML AS 
SELECT *, 'Rule - 1' AS rule_name
FROM daas.o_cds AS T1
WHERE company_country = 'US'
AND company_status = 'whitelist'
;

--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
--
	
INSERT INTO daas.dax_11834_Master_Domains_20250723_ML
WITH base AS
(
	SELECT *
	FROM daas.o_cds AS T1
	WHERE primary_domain NOT IN (SELECT primary_domain FROM daas.dax_11834_Master_Domains_20250723_ML)
	AND company_country = 'US'
AND company_status = 'active'
)
, base2 AS
(
	SELECT primary_domain
	FROM daas.o_cds AS T1
	WHERE primary_domain IN (SELECT primary_domain FROM base)
	AND company_country <> 'US'
	AND company_status IN ('whitelist', 'active')
	GROUP BY 1
	HAVING COUNT(DISTINCT company_status) = 1
		AND COUNT(DISTINCT CASE WHEN company_status = 'active' THEN company_status ELSE NULL END) = 1
)
SELECT *, 'Rule - 2' AS rule_name
FROM base
WHERE primary_domain IN (SELECT primary_domain FROM base2)
;

-- 1789922
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
--

CREATE TABLE daas.DAX_11834_for_Rule3_Temp_20250723_ML
WITH base AS
( 
	SELECT primary_domain
	FROM daas.o_cds AS T1
	WHERE primary_domain NOT IN (SELECT primary_domain FROM daas.dax_11834_Master_Domains_20250723_ML)
	AND company_country = 'US'
	AND company_status IN ('active', 'whitelist') 
)
, base2 AS
(
	SELECT *
	FROM daas.o_cds AS T1
	WHERE primary_domain NOT IN (SELECT primary_domain FROM daas.dax_11834_Master_Domains_20250723_ML)
	AND company_status = 'whitelist'
	AND primary_domain NOT IN (SELECT primary_domain FROM base)
)
SELECT T1.company_id, T1.primary_domain,
COUNT(DISTINCT T2.contactid) AS hvc,
COUNT(DISTINCT T3.person_id) AS mvc,
COUNT(DISTINCT CASE WHEN T2.lvl IN ('brd', 'chf', 'vp', 'dir', 'mgr') THEN T2.contactId ELSE NULL END) AS hvc_mgr_plus,
COUNT(DISTINCT CASE WHEN T3.title_level IN ('brd', 'chf', 'vp', 'dir', 'mgr') THEN T3.person_id ELSE NULL END) AS mvc_mgr_plus
FROM base2 AS T1
LEFT JOIN daas.t0export_20250703 AS T2 ON T1.primary_domain = T2.domain AND COALESCE(T2.primaryCountry, T2.personalCountry, 'US') = T1.company_country AND T2.isSuspended = '0' AND T2.isDeleted = '0'
LEFT JOIN daas.person_top_positions_20250616_fixed_gr AS T3 ON T1.company_id = T3.company_id AND T3.person_source = 'siq'
GROUP BY 1, 2
;

--
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
--
 
INSERT INTO daas.dax_11834_Master_Domains_20250723_ML
WITH base AS
(
	SELECT *, ROW_NUMBER() OVER(PARTITION BY primary_domain ORDER BY hvc DESC, hvc_mgr_plus DESC, mvc_mgr_plus DESC, mvc DESC, company_id) AS rn
	FROM daas.DAX_11834_for_Rule3_Temp_20250723_ML
)
SELECT *, 'Rule - 3' AS rule_name
FROM daas.o_cds
WHERE company_id IN (SELECT company_id FROM base WHERE rn = 1)
; 

-- 1664012
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
--
DROP TABLE daas.DAX_11834_for_Rule3_Temp_20250723_ML;
--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
--

CREATE TABLE daas.DAX_9908_Temp1_ML
SELECT T1.company_id, T1.primary_domain, T1.company_country
FROM daas.o_cds AS T1 
JOIN daas.dax_11834_Master_Domains_20250723_ML AS T2 ON T1.primary_domain = T2.primary_domain AND T1.company_status IN ('whitelist', 'active')
GROUP BY ALL
;

--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
--

CREATE TABLE daas.DAX_9908_Temp_hv_count_ML
SELECT T1.company_id,
COUNT(DISTINCT T2.contactid) AS hvc  
FROM daas.DAX_9908_Temp1_ML AS T1 
JOIN daas.t0export_20250703 AS T2 ON T1.primary_domain = T2.domain AND COALESCE(T2.primaryCountry, T2.personalCountry, 'US') = T1.company_country AND T2.isSuspended = '0' AND T2.isDeleted = '0'
GROUP BY 1
; 

--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
--

CREATE TABLE daas.DAX_9908_Temp_mv_count_ML
SELECT T1.company_id,
COUNT(DISTINCT T3.person_id) AS mvc
FROM daas.DAX_9908_Temp1_ML AS T1
JOIN daas.person_top_positions_20250616_fixed_gr AS T3 ON T1.company_id = T3.company_id AND T3.person_source = 'siq'
GROUP BY 1 
; 
-- 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
--

CREATE TABLE daas.DAX_9908_lookup_for_contact_counts_20250723_ML AS
SELECT T1.company_id, COALESCE(hvc,0) AS hvc, COALESCE(mvc,0) AS mvc
FROM daas.DAX_9908_Temp1_ML AS T1
LEFT JOIN daas.DAX_9908_Temp_hv_count_ML AS T2 ON T1.company_id = T2.company_id
LEFT JOIN daas.DAX_9908_Temp_mv_count_ML AS T3 ON T1.company_id = T3.company_id
;

-- 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
--

DROP TABLE daas.DAX_9908_Temp1_ML;
DROP TABLE daas.DAX_9908_Temp_hv_count_ML;
DROP TABLE daas.DAX_9908_Temp_mv_count_ML;

-- 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
--
SELECT COUNT(*) FROM daas.DAX_9908_lookup_for_contact_counts_20250723_ML ;
-- 11604480
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
--
SELECT *
FROM daas.dax_11834_Master_Domains_20250723_ML
WHERE primary_domain in (SELECT primary_domain FROM (SELECT DISTINCT * EXCEPT (country_Code, digits, location_id) FROM daas.dax_11834_Master_Domains_20250723_ML) AS foo GROUP BY 1 HAVING COUNT(*) > 1)
ORDER BY primary_domain
LIMIT 1000
;  

--
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- EMPLOYEE_COUNT
-- Data to be delivered with this query

SELECT primary_domain as domain , company_country as countryCode, primary_name as name, company_status as status,  new_employee_count as employee_count FROM daas.dax_11834_employee_count_qa_file_20250723_ml; 

--CREATE OR REPLACE TABLE daas.dax_11834_employee_count_qa_file_20250723_ml
WITH CTE
(
	SELECT T2.company_id, T2.primary_domain, T2.company_country, T2.employee_count AS old_employee_count, T2.company_status,T2.primary_name, 
	T1.company_id AS master_company_id, T1.primary_domain AS master_primary_domain, 
	T1.company_country AS master_company_country, T1.employee_count AS new_employee_count, T1.company_status AS master_company_status,
	T3.hvc as  hv_count,
	T3.mvc as mv_count,  
	T1.rule_name  
	FROM daas.dax_11834_Master_Domains_20250723_ML AS T1
	JOIN daas.o_cds T2 ON T1.primary_domain = T2.primary_domain  AND T1.company_country <> T2.company_country AND T1.company_id <> T2.company_id
					 				AND T2.company_status IN ('whitelist', 'active') AND NULLIF(T1.employee_count, 0) IS NOT NULL
									AND T1.employee_count <> COALESCE(T2.employee_count,0) 
	LEFT JOIN daas.DAX_9908_lookup_for_contact_counts_20250723_ML T3 ON T2.company_id = T3.company_id 		
	GROUP BY ALL
)
--SELECT * EXCEPT(rule_name) , hv_count+mv_count AS total_count, rule_name from CTE                               --[QA FILE]
SELECT primary_domain as domain , company_country as countryCode, primary_name as name, company_status as status,  new_employee_count as employee_count from CTE   --[Ingestion File]
ORDER BY domain
;

-- 
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- REVENUE 
-- Data to be delivered with this query 

--CREATE OR REPLACE TABLE daas.dax_11834_revenue_qa_file_20250724_ml
WITH CTE 
(
	SELECT T2.company_id, T2.primary_domain, T2.company_country, BIGINT(ROUND(NULLIF(T2.actual_revenue, '0')::DOUBLE*1E6))  AS old_revenue, T2.company_status, T2.primary_name,
	T1.company_id AS master_company_id, T1.primary_domain AS master_primary_domain,
	T1.company_country AS master_company_country, BIGINT(ROUND(NULLIF(T1.actual_revenue, '0')::DOUBLE*1E6)) AS new_revenue, T1.company_status AS master_company_status,
	T3.hvc as  hv_count, 
	T3.mvc as mv_count,
	T1.rule_name 
	FROM daas.dax_11834_Master_Domains_20250723_ML AS T1 
	JOIN daas.o_cds T2 ON T1.primary_domain = T2.primary_domain  AND T1.company_country <> T2.company_country AND T1.company_id <> T2.company_id
									AND T2.company_status IN ('whitelist', 'active') AND NULLIF(T1.actual_revenue, 0) IS NOT NULL
									AND T1.actual_revenue <> COALESCE(T2.actual_revenue,0) 
	LEFT JOIN daas.DAX_9908_lookup_for_contact_counts_20250723_ML T3 ON T2.company_id = T3.company_id 	
	GROUP BY ALL 
) 
--SELECT DISTINCT * EXCEPT(rule_name) , hv_count+mv_count AS total_count, rule_name from CTE          --[QA FILE]
SELECT DISTINCT primary_domain as domain , company_country as countryCode, primary_name as name, company_status as status, new_revenue as revenue from CTE    --[Ingestion File]  
ORDER BY domain
;

-- 
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
-- LOGO
--Data to be delivered with this query

CREATE OR REPLACE TABLE daas.dax_11834_logo_url_qa_file_20250724_ml AS
WITH CTE 
( 
	SELECT T2.company_id, T2.primary_domain, T2.company_country, T2.logo_url  AS old_logo_url, T2.company_status, T2.primary_name,
	T1.company_id AS master_company_id, T1.primary_domain AS master_primary_domain,
	T1.company_country AS master_company_country, T1.logo_url AS new_logo_url, T1.company_status AS master_company_status,
	T3.hvc as  hv_count, 
	T3.mvc as mv_count, 
	T1.rule_name 
	FROM daas.dax_11834_Master_Domains_20250723_ML AS T1 
	JOIN daas.o_cds T2 ON T1.primary_domain = T2.primary_domain  AND T1.company_country <> T2.company_country AND T1.company_id <> T2.company_id
									AND T2.company_status IN ('whitelist', 'active') AND NULLIF(T1.logo_url, '') IS NOT NULL
									AND T1.logo_url <> COALESCE(T2.logo_url,'') AND T1.logo_url NOT ILIKE '%static%'
	LEFT JOIN daas.DAX_9908_lookup_for_contact_counts_20250723_ML T3 ON T2.company_id = T3.company_id 		
	GROUP BY ALL 
)
--SELECT DISTINCT * EXCEPT(rule_name) , hv_count+mv_count AS total_count, rule_name from CTE            --[QA File]
SELECT DISTINCT primary_domain as domain , company_country as countryCode, primary_name as name, company_status as status, new_logo_url as logo from CTE                      --[Ingestion File]
ORDER BY domain
; 

-- 
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
--

ALTER TABLE daas.dax_11834_Master_Domains_20250723_ML ADD COLUMN naics STRING ;

-- 
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
--

MERGE INTO daas.dax_11834_Master_Domains_20250723_ML AS T1  
USING 
(
	SELECT T1.primary_domain , T1.company_status, T1.company_id,  ARRAY_JOIN(ARRAY_SORT(COLLECT_SET(T2.naics)), '|') as naics 
	FROM daas.dax_11834_Master_Domains_20250723_ML T1
	JOIN daas.o_cds_naics T2 ON T1.company_id = T2.company_id 
	GROUP BY ALL 
) AS T2 ON T1.company_id = T2.company_id AND T1.naics IS NULL
WHEN MATCHED THEN UPDATE SET T1.naics = T2.naics
; 

-- 4856948
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Naics 
-- Data of naics to be delivered with this query
 
--CREATE OR REPLACE TABLE daas.dax_11834_naics_qa_file_20250724_ml AS
WITH CTE
( 
	SELECT T2.company_id, T2.primary_domain, T2.company_country, ARRAY_JOIN(ARRAY_SORT(COLLECT_SET( NULLIF(T4.naics , ''))), '|')  AS old_naics , T2.company_status, T2.primary_name,
	T1.company_id AS master_company_id, T1.primary_domain AS master_primary_domain,
	T1.company_country AS master_company_country, T1.naics AS new_naics, T1.company_status AS master_company_status,
	T3.hvc as  hv_count , 
	T3.mvc as mv_count, 
	T1.rule_name 
	FROM daas.dax_11834_Master_Domains_20250723_ML AS T1 
	JOIN daas.o_cds T2 ON T1.primary_domain = T2.primary_domain  AND T1.company_country <> T2.company_country AND T1.company_id <> T2.company_id
									AND T2.company_status IN ('whitelist', 'active') AND NULLIF(T1.naics, '') IS NOT NULL
	JOIN daas.DAX_9908_lookup_for_contact_counts_20250723_ML T3 ON T2.company_id = T3.company_id 
	LEFT JOIN daas.o_cds_naics T4 ON T2.company_id = T4.company_id 
	GROUP BY ALL 
) 
--SELECT DISTINCT * EXCEPT(rule_name) , hv_count+mv_count AS total_count, rule_name from CTE                                                                      --[QA File] 
SELECT DISTINCT primary_domain as domain , company_country as countryCode, primary_name as name, company_status as status, new_naics AS naics from CTE   --[Ingestion File]
WHERE COALESCE(old_naics,'') <> COALESCE(new_naics,'')
; 

-- 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--

--CREATE OR REPLACE TABLE daas.dax_11834_parent_relationship_data_ml as 
SELECT DISTINCT T1.primary_domain, T2.company_country, T3.relationship_company_domain AS master_relationship_company_domain, T3.relationship AS master_relationship, T2.company_country AS master_relationship_country
FROM daas.dax_11834_Master_Domains_20250723_ML AS T1
JOIN daas.o_cds AS T2 ON T1.primary_domain = T2.primary_domain AND T1.company_country <> T2.company_country AND T2.company_status IN ('whitelist', 'active')
JOIN daas.o_cds_relations AS T3 ON T1.company_id = T3.company_id
; 

--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--

CREATE OR REPLACE TABLE daas.dax_11834_child_relationship_data_ml as 
SELECT DISTINCT T1.primary_domain, T2.company_country, T3.relationship_company_domain AS child_relationship_company_domain, T3.relationship AS child_relationship, T3.relationship_country AS child_relationship_country
FROM daas.dax_11834_Master_Domains_20250723_ML AS T1 
JOIN daas.o_cds AS T2 ON T1.primary_domain = T2.primary_domain AND T1.company_country <> T2.company_country AND T2.company_status IN ('whitelist', 'active')
JOIN daas.o_cds_relations AS T3 ON T2.company_id = T3.company_id
;

--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Relationship
-- Data for relationship to be delivered with this query

--CREATE OR REPLACE TABLE daas.DAX_11834_relationship_data_ml_20250724 AS
WITH base AS                                 
( 
	SELECT T1.primary_domain as domain , T1.company_country as countryCode 
	FROM daas.dax_11834_parent_relationship_data_ml AS T1
	FULL JOIN daas.dax_11834_child_relationship_data_ml AS T2 ON T1.primary_domain = T2.primary_domain AND T1.company_country = T2.company_country
												AND master_relationship_company_domain = child_relationship_company_domain 
												AND master_relationship = child_relationship 
												AND master_relationship_country = child_relationship_country  
	WHERE  
	(
		T1.primary_domain IS NULL
		OR
		T2.primary_domain IS NULL
	)
--	AND T1.primary_domain = 'interiorconceptsinc.com'
	AND NULLIF(T1.company_country,'') IS NOT NULL 
	GROUP BY ALL
)
SELECT
DISTINCT 
primary_domain AS domain,  company_country AS countryCode, master_relationship_company_domain AS relationship_company_domain, master_relationship AS relationship, master_relationship_country AS relationship_country 
FROM daas.dax_11834_parent_relationship_data_ml
WHERE primary_domain||'-'||company_country IN (SELECT primary_domain||'-'||company_country FROM base)
ORDER BY 1, 2
LIMIT 100
;
  
--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--
