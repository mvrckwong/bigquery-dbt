{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='sales_territory_key',
        cluster_by=[
            'region',
            'country',
            'continent'
        ],
        on_schema_change='sync_all_columns',
<<<<<<< HEAD
        tags=['silver', 'territory']
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
        {{ ref('AdventureWorksTerritoryLookup') }} -- Assuming this is your seed file name
    
    {% if is_incremental() %}
    WHERE
        /* Use appropriate incremental logic based on your update pattern */
        SalesTerritoryKey NOT IN (
            SELECT sales_territory_key 
            FROM {{ this }}
        )
    {% endif %}
),

transformed_source AS (
    SELECT
        -- Primary key
        CAST(SalesTerritoryKey AS INT64) AS sales_territory_key
        
        -- Territory attributes
        , TRIM(Region) AS region
        , TRIM(Country) AS country
        , TRIM(Continent) AS continent
        
        -- Metadata
        , {{ var('current_timestamp') }} AS _extracted_at
    FROM 
        source
)

SELECT 
    * 
FROM 
    transformed_source