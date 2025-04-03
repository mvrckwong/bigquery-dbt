{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=[
            'order_date', 
            'order_number', 
            'order_line_item', 
            'product_key'
        ],
        cluster_by=[
            'order_date', 
            'product_key', 
            'customer_key'
        ],
        on_schema_change='sync_all_columns',
<<<<<<< HEAD
        tags=['silver', 'sales']
=======
        tags=[
            'adworks',
            'fact'
        ]
>>>>>>> dev
    )
}}

WITH source_2020 AS (
    SELECT
        *
    FROM 
        {{ ref('AdventureWorksSalesData2020') }}
),

source_2021 AS (
    SELECT
        *
    FROM 
        {{ ref('AdventureWorksSalesData2021') }}
),

source_2022 AS (
    SELECT
        *
    FROM 
        {{ ref('AdventureWorksSalesData2022') }}
),

combined_source AS (
    SELECT * FROM source_2020
    UNION ALL
    SELECT * FROM source_2021
    UNION ALL
    SELECT * FROM source_2022
),

transformed_source AS (
    SELECT
        -- Parse and standardize dates
        CAST(OrderDate AS DATE) AS order_date
        , CAST(StockDate AS DATE) AS stock_date
        
        -- Standardize IDs and values
        , TRIM(OrderNumber) AS order_number
        , CAST(ProductKey AS INT64) AS product_key
        , CAST(CustomerKey AS INT64) AS customer_key
        , CAST(TerritoryKey AS INT64) AS territory_key
        , CAST(OrderLineItem AS INT64) AS order_line_item
        , CAST(OrderQuantity AS INT64) AS order_quantity
        
        -- Metadata
        , {{ var('current_timestamp') }} AS _extracted_at
    FROM 
        combined_source
    WHERE
        OrderDate IS NOT NULL
        AND OrderNumber IS NOT NULL
        AND ProductKey IS NOT NULL
)

SELECT 
    * 
FROM 
    transformed_source

{% if is_incremental() %}
WHERE
    order_date > (
        SELECT 
            MAX(order_date) 
        FROM 
            {{ this }}
    )
{% endif %}