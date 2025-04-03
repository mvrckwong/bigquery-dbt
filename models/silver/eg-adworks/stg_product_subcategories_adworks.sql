{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='product_subcategory_key',
        cluster_by=[
            'subcategory_name',
            'product_category_key',
            'product_subcategory_key'
        ],
        on_schema_change='sync_all_columns',
<<<<<<< HEAD
        tags=['silver', 'product']
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
        {{ ref('AdventureWorksProductSubcategoriesLookup') }} -- Seed file name
    
    {% if is_incremental() %}
    WHERE
        ProductSubcategoryKey NOT IN (
            SELECT product_subcategory_key 
            FROM {{ this }}
        )
    {% endif %}
),

transformed_source AS (
    SELECT
        -- Primary key
        CAST(ProductSubcategoryKey AS INT64) AS product_subcategory_key
        
        -- Product subcategory attributes
        , TRIM(SubcategoryName) AS subcategory_name
        
        -- Foreign key
        , CAST(ProductCategoryKey AS INT64) AS product_category_key
        
        -- Metadata
        , {{ var('current_timestamp') }} AS _extracted_at
    FROM 
        source
)

SELECT 
    * 
FROM 
    transformed_source