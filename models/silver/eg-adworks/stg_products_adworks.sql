{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='product_key',
        cluster_by=[
            'product_name',
            'product_color',
            'product_sku'
        ],
        on_schema_change='sync_all_columns',
        tags=['silver', 'product']
    )
}}

WITH source AS (
    SELECT
        *
    FROM 
        {{ ref('AdventureWorksProductLookup') }} -- Assuming this is your seed file name
    
    {% if is_incremental() %}
    WHERE
        /* Use appropriate incremental logic based on your update pattern */
        ProductKey NOT IN (
            SELECT product_key 
            FROM {{ this }}
        )
    {% endif %}
),

transformed_source AS (
    SELECT
        -- Primary keys and foreign keys
        CAST(ProductKey AS INT64) AS product_key
        , CAST(ProductSubcategoryKey AS INT64) AS product_subcategory_key
        
        -- Product identifiers
        , TRIM(ProductSKU) AS product_sku
        , TRIM(ProductName) AS product_name
        , TRIM(ModelName) AS model_name
        
        -- Product attributes
        , TRIM(ProductDescription) AS product_description
        , TRIM(ProductColor) AS product_color
        , CAST(ProductSize AS STRING) AS product_size
        , CAST(ProductStyle AS STRING) AS product_style
        
        -- Financial attributes
        , CAST(ProductCost AS FLOAT64) AS product_cost
        , CAST(ProductPrice AS FLOAT64) AS product_price
        
        -- Calculated fields
        , ROUND(CAST(ProductPrice AS FLOAT64) - CAST(ProductCost AS FLOAT64), 2) AS product_margin
        , ROUND((CAST(ProductPrice AS FLOAT64) - CAST(ProductCost AS FLOAT64)) / CAST(ProductPrice AS FLOAT64) * 100, 2) AS product_margin_pct
        
        -- Metadata
        , {{ var('current_timestamp') }} AS __extracted_at
    FROM 
        source
)

SELECT 
    * 
FROM 
    transformed_source