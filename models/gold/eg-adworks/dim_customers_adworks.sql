{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='customer_key',
        on_schema_change='sync_all_columns',
        partition_by={
            "field": "_valid_from"
            , "data_type": "timestamp"
            , "granularity": "day"
        },
        cluster_by=[
            'last_name',
            'first_name',
            'email_address'
        ],
        tags=['dimension', 'customer']
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
        
        -- Full name concatenation
        , CONCAT(s.prefix, ' ', s.first_name, ' ', s.last_name) AS full_name
        
        -- Enriched marital status
        , CASE 
            WHEN UPPER(s.marital_status) = 'M' THEN 'Married'
            WHEN UPPER(s.marital_status) = 'S' THEN 'Single'
            ELSE COALESCE(s.marital_status, 'Unknown')
          END AS marital_status_desc
        , CASE 
            WHEN UPPER(s.marital_status) = 'M' THEN 1
            WHEN UPPER(s.marital_status) = 'S' THEN 2
            ELSE 99
          END AS marital_status_sort
        
        -- Enriched gender
        , CASE 
            WHEN UPPER(s.gender) = 'M' THEN 'Male'
            WHEN UPPER(s.gender) = 'F' THEN 'Female'
            ELSE COALESCE(s.gender, 'Unknown')
          END AS gender_desc
        , CASE 
            WHEN UPPER(s.gender) = 'M' THEN 1
            WHEN UPPER(s.gender) = 'F' THEN 2
            ELSE 99
          END AS gender_sort
        
        -- Income tier categorization
        , CASE
            WHEN s.annual_income < 40000 THEN 'Low'
            WHEN s.annual_income BETWEEN 40000 AND 80000 THEN 'Medium'
            WHEN s.annual_income > 80000 THEN 'High'
            ELSE 'Unknown'
          END AS income_tier
        , CASE
            WHEN s.annual_income < 40000 THEN 1
            WHEN s.annual_income BETWEEN 40000 AND 80000 THEN 2
            WHEN s.annual_income > 80000 THEN 3
            ELSE 99
          END AS income_tier_sort
        
        -- Education level enrichment
        , CASE
            WHEN s.education_level = 'Bachelors' THEN 'Bachelors Degree'
            WHEN s.education_level = 'High School' THEN 'High School Diploma'
            WHEN s.education_level = 'Partial College' THEN 'Some College'
            WHEN s.education_level = 'Graduate Degree' THEN 'Graduate Degree'
            WHEN s.education_level = 'Partial High School' THEN 'Some High School'
            ELSE COALESCE(s.education_level, 'Unknown')
          END AS education_level_desc
        , CASE
            WHEN s.education_level = 'Partial High School' THEN 1
            WHEN s.education_level = 'High School' THEN 2
            WHEN s.education_level = 'Partial College' THEN 3
            WHEN s.education_level = 'Bachelors' THEN 4
            WHEN s.education_level = 'Graduate Degree' THEN 5
            ELSE 99
          END AS education_level_sort
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
        , e.full_name
        , e.birth_date
        , e.age
        , e.marital_status
        , e.marital_status_desc
        , e.marital_status_sort
        , e.gender
        , e.gender_desc
        , e.gender_sort
        , e.email_address
        , e.annual_income
        , e.income_tier
        , e.income_tier_sort
        , e.total_children
        , e.education_level
        , e.education_level_desc
        , e.education_level_sort
        , e.occupation
        , e.is_home_owner
        
        -- SCD metadata
        , e.dbt_valid_from AS _valid_from
        , e.dbt_valid_to AS _valid_to
        , e.is_current AS _is_current
    FROM 
        enriched e
)

SELECT 
    * 
FROM 
    final