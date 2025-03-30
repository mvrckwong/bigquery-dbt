{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='customer_key',
        on_schema_change='sync_all_columns',
        partition_by={
            "field": "_valid_from",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=[
            'last_name',
            'income_tier_sort'
        ],
        tags=['dimension', 'adworks']
    )
}}

WITH source AS (
    SELECT
        {{ dbt_utils.star(from=ref('snap_customers_adworks')) }}
        , (dbt_valid_to IS NULL) AS is_current
    FROM 
        {{ ref('snap_customers_adworks') }}
    
    {% if is_incremental() %}
    WHERE
        dbt_valid_from > (
            SELECT MAX(_valid_from) 
            FROM {{ this }}
        )
    {% endif %}
),

-- Enriched data with additional attributes and calculations
enriched AS (
    SELECT
        s.*
        
        -- Age calculation
        , DATE_DIFF(CURRENT_DATE(), s.birth_date, YEAR) AS age
        
        -- Age group categorization
        , CASE
            WHEN DATE_DIFF(CURRENT_DATE(), s.birth_date, YEAR) < 18 THEN 'Under 18'
            WHEN DATE_DIFF(CURRENT_DATE(), s.birth_date, YEAR) BETWEEN 18 AND 24 THEN '18-24'
            WHEN DATE_DIFF(CURRENT_DATE(), s.birth_date, YEAR) BETWEEN 25 AND 34 THEN '25-34'
            WHEN DATE_DIFF(CURRENT_DATE(), s.birth_date, YEAR) BETWEEN 35 AND 44 THEN '35-44'
            WHEN DATE_DIFF(CURRENT_DATE(), s.birth_date, YEAR) BETWEEN 45 AND 54 THEN '45-54'
            WHEN DATE_DIFF(CURRENT_DATE(), s.birth_date, YEAR) BETWEEN 55 AND 64 THEN '55-64'
            WHEN DATE_DIFF(CURRENT_DATE(), s.birth_date, YEAR) BETWEEN 65 AND 74 THEN '65-74'
            WHEN DATE_DIFF(CURRENT_DATE(), s.birth_date, YEAR) >= 75 THEN '75+'
            ELSE 'Unknown'
          END AS age_group
        , CASE
            WHEN DATE_DIFF(CURRENT_DATE(), s.birth_date, YEAR) < 18 THEN 1
            WHEN DATE_DIFF(CURRENT_DATE(), s.birth_date, YEAR) BETWEEN 18 AND 24 THEN 2
            WHEN DATE_DIFF(CURRENT_DATE(), s.birth_date, YEAR) BETWEEN 25 AND 34 THEN 3
            WHEN DATE_DIFF(CURRENT_DATE(), s.birth_date, YEAR) BETWEEN 35 AND 44 THEN 4
            WHEN DATE_DIFF(CURRENT_DATE(), s.birth_date, YEAR) BETWEEN 45 AND 54 THEN 5
            WHEN DATE_DIFF(CURRENT_DATE(), s.birth_date, YEAR) BETWEEN 55 AND 64 THEN 6
            WHEN DATE_DIFF(CURRENT_DATE(), s.birth_date, YEAR) BETWEEN 65 AND 74 THEN 7
            WHEN DATE_DIFF(CURRENT_DATE(), s.birth_date, YEAR) >= 75 THEN 8
            ELSE 99
          END AS age_group_sort
        
        -- Name formatting
        , TRIM(CONCAT(COALESCE(s.prefix, ''), ' ', s.first_name, ' ', s.last_name)) AS full_name
        , TRIM(CONCAT(s.first_name, ' ', s.last_name)) AS display_name
        
        -- Email domain extraction
        , CASE
            WHEN s.email_address LIKE '%@%' 
            THEN TRIM(SPLIT(s.email_address, '@')[SAFE_OFFSET(1)])
            ELSE NULL
          END AS email_domain
        
        -- Income tier categorization 
        , CASE
            WHEN s.annual_income < 30000 THEN 'Low Income'
            WHEN s.annual_income BETWEEN 30000 AND 69999 THEN 'Middle Income'
            WHEN s.annual_income BETWEEN 70000 AND 119999 THEN 'Upper Middle Income'
            WHEN s.annual_income >= 120000 THEN 'High Income'
            ELSE 'Unknown'
          END AS income_tier
        , CASE
            WHEN s.annual_income < 30000 THEN 1
            WHEN s.annual_income BETWEEN 30000 AND 69999 THEN 2
            WHEN s.annual_income BETWEEN 70000 AND 119999 THEN 3
            WHEN s.annual_income >= 120000 THEN 4
            ELSE 99
          END AS income_tier_sort

        -- SCD metadata - date components
        , EXTRACT(YEAR FROM s.dbt_valid_from) AS _valid_year
        , EXTRACT(MONTH FROM s.dbt_valid_from) AS _valid_month
    FROM 
        source s
),

-- Final dimension table with restructured columns
final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(
            ['e.customer_key', 'e.dbt_valid_from']
        ) }} AS customer_key
        
        -- Original customer attributes
        , e.customer_key AS source_customer_key
        , e.prefix
        , e.first_name
        , e.last_name
        , e.display_name
        , e.full_name
        , e.birth_date
        , e.age
        , e.age_group
        , e.age_group_sort
        , e.marital_status
        , e.gender
        , e.email_address
        , e.email_domain
        , e.annual_income
        , e.income_tier
        , e.income_tier_sort
        , e.total_children
        , e.education_level
        , e.occupation
        , e.is_home_owner
        
        -- Customer segmentation (derived from multiple attributes)
        , CASE 
            WHEN e.annual_income >= 100000 OR (e.is_home_owner = TRUE AND e.annual_income >= 70000) THEN 'Premium'
            WHEN e.annual_income >= 50000 OR (e.is_home_owner = TRUE AND e.education_level IN ('Bachelors', 'Graduate Degree')) THEN 'Standard'
            ELSE 'Basic'
          END AS customer_segment
        , CASE 
            WHEN e.annual_income >= 100000 OR (e.is_home_owner = TRUE AND e.annual_income >= 70000) THEN 1
            WHEN e.annual_income >= 50000 OR (e.is_home_owner = TRUE AND e.education_level IN ('Bachelors', 'Graduate Degree')) THEN 2
            ELSE 3
          END AS customer_segment_sort
        
        -- SCD metadata
        , e.dbt_valid_from AS _valid_from
        , e.dbt_valid_to AS _valid_to
        , e.is_current AS _is_current
        , e._valid_year
        , e._valid_month
    FROM 
        enriched e
)

SELECT 
    * 
FROM 
    final