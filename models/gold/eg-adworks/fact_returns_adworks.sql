{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='return_key',
        partition_by={
            "field": "return_date"
            , "data_type": "date"
            , "granularity": "day"
        },
        cluster_by=[
            'product_key',
            'territory_key',
            'return_quantity'
        ],
        on_schema_change='sync_all_columns',
        tags=['gold', 'returns_fact']
    )
}}

WITH source AS (
    SELECT
        {{ dbt_utils.star(from=ref('stg_returns_adworks')) }}
    FROM 
        {{ ref('stg_returns_adworks') }}
    
    {% if is_incremental() %}
    WHERE
        return_date > (
            SELECT MAX(return_date) 
            FROM {{ this }}
        )
    {% endif %}
),

transformed_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(
            ['s.return_date', 's.product_key', 's.territory_key', 's.return_quantity']
        ) }} AS return_key
        
        -- All return fact columns
        , s.return_date
        , s.product_key
        , s.territory_key
        , s.return_quantity
        
        -- Foreign keys to dimensions
        , {{ dbt_utils.generate_surrogate_key(['s.product_key']) }} AS product_dim_key
        , {{ dbt_utils.generate_surrogate_key(['s.territory_key']) }} AS territory_dim_key
        
        -- Metadata
        , s._extracted_at
    FROM 
        source s
),

metrics AS (
    SELECT
        t.*
        
        -- Business metrics calculations
        , EXTRACT(YEAR FROM t.return_date) AS return_year
        , EXTRACT(MONTH FROM t.return_date) AS return_month
        , EXTRACT(DAY FROM t.return_date) AS return_day
        , CASE
            WHEN EXTRACT(DAYOFWEEK FROM t.return_date) IN (1, 7) THEN 'Weekend'
            ELSE 'Weekday'
          END AS day_type
        , CASE
            WHEN EXTRACT(DAYOFWEEK FROM t.return_date) = 1 THEN 'Sunday'
            WHEN EXTRACT(DAYOFWEEK FROM t.return_date) = 2 THEN 'Monday'
            WHEN EXTRACT(DAYOFWEEK FROM t.return_date) = 3 THEN 'Tuesday'
            WHEN EXTRACT(DAYOFWEEK FROM t.return_date) = 4 THEN 'Wednesday'
            WHEN EXTRACT(DAYOFWEEK FROM t.return_date) = 5 THEN 'Thursday'
            WHEN EXTRACT(DAYOFWEEK FROM t.return_date) = 6 THEN 'Friday'
            WHEN EXTRACT(DAYOFWEEK FROM t.return_date) = 7 THEN 'Saturday'
          END AS day_name
        , CASE
            WHEN EXTRACT(DAYOFWEEK FROM t.return_date) = 1 THEN 7
            WHEN EXTRACT(DAYOFWEEK FROM t.return_date) = 2 THEN 1
            WHEN EXTRACT(DAYOFWEEK FROM t.return_date) = 3 THEN 2
            WHEN EXTRACT(DAYOFWEEK FROM t.return_date) = 4 THEN 3
            WHEN EXTRACT(DAYOFWEEK FROM t.return_date) = 5 THEN 4
            WHEN EXTRACT(DAYOFWEEK FROM t.return_date) = 6 THEN 5
            WHEN EXTRACT(DAYOFWEEK FROM t.return_date) = 7 THEN 6
          END AS day_name_sort
    FROM 
        transformed_data t
)

SELECT 
    * 
FROM 
    metrics