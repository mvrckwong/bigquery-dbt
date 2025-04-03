{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='sale_key',
        partition_by={
            "field": "order_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=[
            'product_key',
            'customer_key',
            'territory_key'
        ],
        on_schema_change='sync_all_columns',
        tags=[
            'adworks',
            'fact'
        ]
    )
}}

WITH source AS (
    SELECT
        {{ dbt_utils.star(from=ref('stg_sales_adworks')) }}
    FROM 
        {{ ref('stg_sales_adworks') }}
    
    {% if is_incremental() %}
    WHERE
        order_date > (
            SELECT MAX(order_date) 
            FROM {{ this }}
        )
    {% endif %}
),

-- Enriched data with additional attributes and calculations
enriched AS (
    SELECT
        s.*
        
        -- Calculate lead time (days between stock and order)
        , DATE_DIFF(s.order_date, s.stock_date, DAY) AS lead_time_days
          
        -- Order fulfillment categorization
        , CASE
            WHEN DATE_DIFF(s.order_date, s.stock_date, DAY) <= 7 THEN 'Quick'
            WHEN DATE_DIFF(s.order_date, s.stock_date, DAY) <= 30 THEN 'Normal'
            ELSE 'Extended'
          END AS fulfillment_category
        , CASE
            WHEN DATE_DIFF(s.order_date, s.stock_date, DAY) <= 7 THEN 1
            WHEN DATE_DIFF(s.order_date, s.stock_date, DAY) <= 30 THEN 2
            ELSE 3
          END AS fulfillment_category_sort
    FROM 
        source s
),

-- Final fact table with restructured columns
final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(
            ['e.order_date', 'e.order_number', 'e.order_line_item', 'e.product_key']
        ) }} AS sale_key
        
        -- Original sales attributes
        , e.order_date
        , e.stock_date
        , e.order_number
        , e.order_line_item
        , e.product_key
        , e.customer_key
        , e.territory_key
        , e.order_quantity
        
        -- Foreign keys to dimensions
        , {{ dbt_utils.generate_surrogate_key(['e.product_key']) }} AS product_dim_key
        , {{ dbt_utils.generate_surrogate_key(['e.customer_key']) }} AS customer_dim_key
        , {{ dbt_utils.generate_surrogate_key(['e.territory_key']) }} AS territory_dim_key
        
        -- Enriched time attributes
        , e.lead_time_days
        
        -- Fulfillment attributes
        , e.fulfillment_category
        , e.fulfillment_category_sort
        
        -- Metadata
        , e._extracted_at AS _loaded_at
    FROM 
        enriched e
)

SELECT 
    * 
FROM 
    final