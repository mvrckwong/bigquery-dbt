{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='product_category_key',
        cluster_by=[
            'product_category_key',
            'category_name',
            '_extracted_at'
        ],
        on_schema_change='sync_all_columns',
<<<<<<< HEAD
        tags=['silver', 'product_category']
=======
        tags=[
            'adworks',
            'dimension'
        ]
>>>>>>> dev
    )
}}

WITH source AS (
    SELECT
        *
    FROM 
        {{ ref('AdventureWorksProductCategoriesLookup') }}  -- Assuming this is your seed file name
    
    {% if is_incremental() %}
    WHERE
        ProductCategoryKey NOT IN (
            SELECT product_category_key 
            FROM {{ this }}
        )
    {% endif %}
),

transformed_source AS (
    SELECT
        -- Primary key
        CAST(ProductCategoryKey AS INT64) AS product_category_key
        
        -- Attributes
        , TRIM(CategoryName) AS category_name
        
        -- Metadata
        , {{ var('current_timestamp') }} AS _extracted_at
    FROM 
        source
)

SELECT 
    * 
FROM 
    transformed_source