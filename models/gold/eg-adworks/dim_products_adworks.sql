{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='product_key',
        on_schema_change='sync_all_columns',
        partition_by={
            "field": "_valid_from",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=[
            'product_brand',
            'product_color',
            'product_sku'
        ],
        tags=['dimension', 'adworks']
    )
}}

WITH source AS (
    SELECT
        {{ dbt_utils.star(from=ref('snap_products_adworks')) }}
        , (dbt_valid_to IS NULL) AS is_current
    FROM 
        {{ ref('snap_products_adworks') }}
    
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
        
        -- Create product_brand from model_name
        , TRIM(SPLIT(s.model_name, '-')[SAFE_OFFSET(0)]) AS product_brand
        
        -- Calculated fields (renamed to avoid ambiguity)
        , ROUND(s.product_price - s.product_cost, 2) AS calculated_margin
        , ROUND((s.product_price - s.product_cost) / s.product_price * 100, 2) AS calculated_margin_pct
        
        -- Enriched size attributes
        , CASE 
            WHEN UPPER(s.product_size) = 'S' THEN 'Small'
            WHEN UPPER(s.product_size) = 'M' THEN 'Medium'
            WHEN UPPER(s.product_size) = 'L' THEN 'Large'
            WHEN s.product_size = '0' THEN 'Universal'
            ELSE COALESCE(s.product_size, 'Unknown')
          END AS product_size_desc
        , CASE 
            WHEN UPPER(s.product_size) = 'S' THEN 1
            WHEN UPPER(s.product_size) = 'M' THEN 2
            WHEN UPPER(s.product_size) = 'L' THEN 3
            WHEN s.product_size = '0' THEN 4
            ELSE 99
          END AS product_size_sort
        
        -- Enriched style attributes
        , CASE 
            WHEN s.product_style = 'U' THEN 'Unisex'
            WHEN s.product_style = 'M' THEN 'Mens'
            WHEN s.product_style = 'W' THEN 'Womens'
            ELSE COALESCE(s.product_style, 'Unknown')
          END AS product_style_desc
        
        -- Enriched price category
        , CASE
            WHEN s.product_price < 10 THEN 'Budget'
            WHEN s.product_price BETWEEN 10 AND 50 THEN 'Standard'
            WHEN s.product_price > 50 THEN 'Premium'
            ELSE 'Unknown'
          END AS price_category
        , CASE
            WHEN s.product_price < 10 THEN 1
            WHEN s.product_price BETWEEN 10 AND 50 THEN 2
            WHEN s.product_price > 50 THEN 3
            ELSE 99
          END AS price_category_sort
    FROM 
        source s
),

-- Final dimension table with restructured columns
final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(
            ['e.product_key', 'e.dbt_valid_from']
        ) }} AS product_key
        
        -- Original product attributes
        , e.product_key AS source_product_key
        , e.product_subcategory_key
        , e.product_sku
        , e.product_name
        , e.model_name
        , e.product_description
        , e.product_brand  -- Added derived product_brand
        , e.product_color
        , e.product_size
        , e.product_style
        , e.product_cost
        , e.product_price
        
        -- Enriched calculations
        , e.calculated_margin AS product_margin
        , e.calculated_margin_pct AS product_margin_pct
        
        -- Enriched categorizations
        , e.product_size_desc
        , e.product_size_sort
        , e.product_style_desc
        , e.price_category
        , e.price_category_sort
        
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