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

-- Get the product dimension data with proper filtering
product_data AS (
    SELECT
        source_product_key,  -- This is the original product_key from source
        product_price,
        product_cost,
        product_margin,
        product_margin_pct
    FROM
        {{ ref('dim_products_adworks') }}
    WHERE
        _is_current = TRUE  -- Using only current product records
),

-- Enriched data with additional attributes and calculations
enriched AS (
    SELECT
        s.*
        -- Calculate lead time (days between stock and order)
        , DATE_DIFF(s.order_date, s.stock_date, DAY) AS lead_time_days
        -- Order fulfillment categorization
        , CASE
            WHEN DATE_DIFF(s.order_date, s.stock_date, DAY) <= 14 THEN 'Quick'
            WHEN DATE_DIFF(s.order_date, s.stock_date, DAY) <= 30 THEN 'Normal'
            ELSE 'Extended'
          END AS fulfillment_category
        , CASE
            WHEN DATE_DIFF(s.order_date, s.stock_date, DAY) <= 14 THEN 1
            WHEN DATE_DIFF(s.order_date, s.stock_date, DAY) <= 30 THEN 2
            ELSE 3
          END AS fulfillment_category_sort
        -- Join product data for revenue calculations
        , COALESCE(p.product_price, 0) AS product_price
        , COALESCE(p.product_cost, 0) AS product_cost
        -- Calculate revenue with NULL handling
        , CASE 
            WHEN p.product_price IS NOT NULL THEN ROUND(s.order_quantity * p.product_price, 2)
            ELSE 0
          END AS revenue
        -- Calculate total cost with NULL handling
        , CASE 
            WHEN p.product_cost IS NOT NULL THEN ROUND(s.order_quantity * p.product_cost, 2)
            ELSE 0
          END AS total_cost
        -- Calculate profit with NULL handling
        , CASE 
            WHEN p.product_price IS NOT NULL AND p.product_cost IS NOT NULL 
            THEN ROUND(s.order_quantity * (p.product_price - p.product_cost), 2)
            ELSE 0
          END AS profit
        -- Calculate margin percentage with NULL and division by zero handling
        , CASE 
            WHEN p.product_price IS NOT NULL AND p.product_cost IS NOT NULL AND p.product_price > 0
            THEN ROUND(
                (s.order_quantity * (p.product_price - p.product_cost)) / 
                (s.order_quantity * p.product_price) * 100, 2)
            ELSE 0
          END AS transaction_margin_pct
    FROM
        source s
    LEFT JOIN
        product_data p
    ON
        s.product_key = p.source_product_key
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
        -- Revenue calculations
        , e.product_price
        , e.product_cost
        , e.revenue
        , e.total_cost
        , e.profit
        , e.transaction_margin_pct
        -- Metadata
        , e._extracted_at AS _loaded_at
    FROM
        enriched e
)

SELECT
    *
FROM
    final