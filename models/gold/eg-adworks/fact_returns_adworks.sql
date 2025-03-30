{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='return_key',
        partition_by={
            "field": "return_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=[
            'product_key',
            'territory_key',
            'return_quantity'
        ],
        on_schema_change='sync_all_columns',
        tags=['fact', 'adworks']
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
    FROM 
        transformed_data t
)

SELECT 
    * 
FROM 
    metrics