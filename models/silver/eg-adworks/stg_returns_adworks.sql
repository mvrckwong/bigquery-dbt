{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=[
            'return_date', 
            'territory_key', 
            'product_key'
        ],
        cluster_by=[
            'return_date', 
            'territory_key', 
            'product_key'
        ],
        on_schema_change='sync_all_columns',
        tags=['silver', 'returns']
    )
}}

WITH source AS (
    SELECT
        *
    FROM 
        {{ ref('AdventureWorksReturnsData') }}
    
    {% if is_incremental() %}
    WHERE
        -- Use parsed return_date for incremental loads
        CAST(ReturnDate AS DATE) > (
            SELECT 
                MAX(return_date) 
            FROM 
                {{ this }}
        )
    {% endif %}
),

transformed_source AS (
    SELECT
        -- Parse and standardize dates
        CAST(ReturnDate AS DATE) AS return_date
        
        -- Standardize IDs as integers
        , CAST(TerritoryKey AS INT64) AS territory_key
        , CAST(ProductKey AS INT64) AS product_key
        , CAST(ReturnQuantity AS INT64) AS return_quantity
        
        -- Metadata
        , {{ var('current_timestamp') }} AS _extracted_at
    FROM 
        source
    WHERE
        ReturnDate IS NOT NULL
        AND TerritoryKey IS NOT NULL
        AND ProductKey IS NOT NULL
)

SELECT 
    * 
FROM 
    transformed_source