{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='product_subcategory_key',
        on_schema_change='sync_all_columns',
        partition_by={
            "field": "_valid_from"
            , "data_type": "timestamp"
            , "granularity": "day"
        },
        cluster_by=[
            'subcategory_name',
            'product_category_key',
            'subcategory_type'
        ],
        tags=['dimension']
    )
}}

WITH source AS (
    SELECT
        {{ dbt_utils.star(from=ref('snap_product_subcategories_adworks')) }}
        , (dbt_valid_to IS NULL) AS _is_current
    FROM 
        {{ ref('snap_product_subcategories_adworks') }}
    
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
        
        -- Create subcategory_type based on product_category_key
        , CASE 
            WHEN s.product_category_key = 1 THEN 'Bike'
            WHEN s.product_category_key = 2 THEN 'Component'
            WHEN s.product_category_key = 3 THEN 'Clothing'
            WHEN s.product_category_key = 4 THEN 'Accessory'
            ELSE 'Unknown'
          END AS subcategory_type
        
        -- Subcategory type sort order
        , CASE 
            WHEN s.product_category_key = 1 THEN 1
            WHEN s.product_category_key = 2 THEN 2
            WHEN s.product_category_key = 3 THEN 3
            WHEN s.product_category_key = 4 THEN 4
            ELSE 99
          END AS subcategory_type_sort
    FROM 
        source s
),

-- Final dimension table with restructured columns
final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(
            ['e.product_subcategory_key', 'e.dbt_valid_from']
        ) }} AS product_subcategory_key
        
        -- Original subcategory attributes
        , e.product_subcategory_key AS source_product_subcategory_key
        , e.subcategory_name
        , e.product_category_key
        
        -- Enriched categorizations
        , e.subcategory_type
        , e.subcategory_type_sort
        
        -- SCD metadata
        , e.dbt_valid_from AS _valid_from
        , e.dbt_valid_to AS _valid_to
        , e._is_current
    FROM 
        enriched e
)

SELECT 
    * 
FROM 
    final