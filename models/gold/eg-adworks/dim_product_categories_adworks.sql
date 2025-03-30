{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='product_category_key',
        on_schema_change='sync_all_columns',
        partition_by={
            "field": "_valid_from",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=[
            'product_category_key',
            'category_name',
            '_is_current'
        ],
        tags=['dimension', 'adworks']
    )
}}

WITH source AS (
    SELECT
        {{ dbt_utils.star(from=ref('snap_product_categories_adworks')) }}
        , (dbt_valid_to IS NULL) AS is_current
    FROM 
        {{ ref('snap_product_categories_adworks') }}
    
    {% if is_incremental() %}
    WHERE
        dbt_valid_from > (
            SELECT MAX(_valid_from) 
            FROM {{ this }}
        )
    {% endif %}
),

-- Enriched data with additional attributes and categorizations
enriched AS (
    SELECT
        s.*
        
        -- Enriched category attributes
        , CASE 
            WHEN LOWER(s.category_name) = 'bikes' THEN 'Transportation'
            WHEN LOWER(s.category_name) = 'components' THEN 'Parts'
            WHEN LOWER(s.category_name) = 'clothing' THEN 'Apparel'
            WHEN LOWER(s.category_name) = 'accessories' THEN 'Add-ons'
            ELSE 'Other'
          END AS category_group
        , CASE 
            WHEN LOWER(s.category_name) = 'bikes' THEN 1
            WHEN LOWER(s.category_name) = 'components' THEN 2
            WHEN LOWER(s.category_name) = 'clothing' THEN 3
            WHEN LOWER(s.category_name) = 'accessories' THEN 4
            ELSE 99
          END AS category_group_sort
          
        -- Additional enrichments - business classification
        , CASE
            WHEN LOWER(s.category_name) IN ('bikes', 'components') THEN 'Core Products'
            ELSE 'Supporting Products'
          END AS product_classification
    FROM 
        source s
),

-- Final dimension table with restructured columns
final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(
            ['e.product_category_key', 'e.dbt_valid_from']
        ) }} AS product_category_key
        
        -- Original attributes
        , e.product_category_key AS source_category_key
        , e.category_name
        
        -- Enrichments
        , e.category_group
        , e.category_group_sort
        , e.product_classification
        
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