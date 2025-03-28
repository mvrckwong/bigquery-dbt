{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='date_id',
        cluster_by=[
            'calendar_date',
            'date_id',
            '_extracted_at'
        ],
        on_schema_change='sync_all_columns',
        tags=['silver', 'calendar']
    )
}}

WITH source AS (
    SELECT
        *
    FROM 
        {{ ref('AdventureWorksCalendarLookup') }} -- Assuming this is your seed file name
    
    {% if is_incremental() %}
    WHERE
        Date > (
            SELECT MAX(calendar_date) 
            FROM {{ this }}
        )
    {% endif %}
),

transformed_source AS (
    SELECT
        -- Primary key
        ROW_NUMBER() OVER (ORDER BY CAST(Date AS DATE)) AS date_id
        
        -- Basic cleaning and type conversion
        , CAST(Date AS DATE) AS calendar_date
        
        -- Metadata
        , {{ var('current_timestamp') }} AS _extracted_at
    FROM 
        source
    WHERE
        Date IS NOT NULL -- Basic data cleaning
)

SELECT 
    * 
FROM 
    transformed_source