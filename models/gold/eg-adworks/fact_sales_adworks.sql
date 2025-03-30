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
        tags=['fact', 'adworks']
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
        
        -- Extract date components for analysis
        , EXTRACT(YEAR FROM s.order_date) AS order_year
        , EXTRACT(MONTH FROM s.order_date) AS order_month
        , EXTRACT(DAY FROM s.order_date) AS order_day
        , EXTRACT(QUARTER FROM s.order_date) AS order_quarter
        
        -- Day of week enrichment
        , CASE
            WHEN EXTRACT(DAYOFWEEK FROM s.order_date) = 1 THEN 'Sunday'
            WHEN EXTRACT(DAYOFWEEK FROM s.order_date) = 2 THEN 'Monday'
            WHEN EXTRACT(DAYOFWEEK FROM s.order_date) = 3 THEN 'Tuesday'
            WHEN EXTRACT(DAYOFWEEK FROM s.order_date) = 4 THEN 'Wednesday'
            WHEN EXTRACT(DAYOFWEEK FROM s.order_date) = 5 THEN 'Thursday'
            WHEN EXTRACT(DAYOFWEEK FROM s.order_date) = 6 THEN 'Friday'
            WHEN EXTRACT(DAYOFWEEK FROM s.order_date) = 7 THEN 'Saturday'
          END AS day_name
        , CASE
            WHEN EXTRACT(DAYOFWEEK FROM s.order_date) = 1 THEN 7
            WHEN EXTRACT(DAYOFWEEK FROM s.order_date) = 2 THEN 1
            WHEN EXTRACT(DAYOFWEEK FROM s.order_date) = 3 THEN 2
            WHEN EXTRACT(DAYOFWEEK FROM s.order_date) = 4 THEN 3
            WHEN EXTRACT(DAYOFWEEK FROM s.order_date) = 5 THEN 4
            WHEN EXTRACT(DAYOFWEEK FROM s.order_date) = 6 THEN 5
            WHEN EXTRACT(DAYOFWEEK FROM s.order_date) = 7 THEN 6
          END AS day_name_sort
        
        -- Weekend or weekday flag
        , CASE
            WHEN EXTRACT(DAYOFWEEK FROM s.order_date) IN (1, 7) THEN 'Weekend'
            ELSE 'Weekday'
          END AS day_type
        , CASE
            WHEN EXTRACT(DAYOFWEEK FROM s.order_date) IN (1, 7) THEN 1
            ELSE 0
          END AS is_weekend
          
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
        
        -- Date components for analysis
        , e.order_year
        , e.order_month
        , e.order_day
        , e.order_quarter
        
        -- Enriched time attributes
        , e.lead_time_days
        , e.day_name
        , e.day_name_sort
        , e.day_type
        , e.is_weekend
        
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