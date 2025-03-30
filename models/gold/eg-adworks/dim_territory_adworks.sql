{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='territory_key',
        on_schema_change='sync_all_columns',
        partition_by={
            "field": "_valid_from",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=[
            'region',
            'country',
            'continent'
        ],
        tags=['dimension', 'adworks']
    )
}}

WITH source AS (
    SELECT
        {{ dbt_utils.star(from=ref('snap_territories_adworks')) }}
        , (dbt_valid_to IS NULL) AS is_current
    FROM 
        {{ ref('snap_territories_adworks') }}
    
    {% if is_incremental() %}
    WHERE
        dbt_valid_from > (
            SELECT MAX(_valid_from) 
            FROM {{ this }}
        )
    {% endif %}
),

-- Enriched data with additional attributes and calculations
enriched AS (
    SELECT
        s.*
        
        -- Enriched continent attributes
        , CASE 
            WHEN s.continent = 'North America' THEN 'NA'
            WHEN s.continent = 'South America' THEN 'SA'
            WHEN s.continent = 'Europe' THEN 'EU'
            WHEN s.continent = 'Asia' THEN 'AS'
            WHEN s.continent = 'Africa' THEN 'AF'
            WHEN s.continent = 'Oceania' THEN 'OC'
            ELSE 'Unknown'
          END AS continent_code
        
        -- Territory size categorization
        , CASE 
            WHEN s.region IN ('Northwest', 'Northeast', 'Central') THEN 'Core'
            ELSE 'Expansion'
          END AS territory_category
        
        -- Domestic vs international
        , CASE
            WHEN s.country = 'United States' THEN 'Domestic'
            ELSE 'International'
          END AS market_type
        
        -- Domestic region sorting
        , CASE
            WHEN s.region = 'Northwest' THEN 1
            WHEN s.region = 'Northeast' THEN 2
            WHEN s.region = 'Central' THEN 3
            ELSE 99
          END AS region_sort
    FROM 
        source s
),

-- Final dimension table with restructured columns
final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(
            ['e.sales_territory_key', 'e.dbt_valid_from']
        ) }} AS territory_key
        
        -- Original territory attributes
        , e.sales_territory_key AS source_territory_key
        , e.region
        , e.country
        , e.continent
        
        -- Enriched attributes
        , e.continent_code
        , e.territory_category
        , e.market_type
        , e.region_sort
        
        -- Indicators
        , (e.country = 'United States') AS is_domestic
        
        -- SCD metadata
        , e.dbt_valid_from AS _valid_from
        , e.dbt_valid_to AS _valid_to
        , e.is_current AS _is_current
    FROM 
        enriched e
)

SELECT 
    * 
FROM 
    final