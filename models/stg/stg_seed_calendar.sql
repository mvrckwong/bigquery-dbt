{{
	config(
		materialized='table',
		database='stg'
	) 
}}

WITH source AS (
      SELECT
		*
      FROM 
            {{ source('bronze', 'raw_seed_calendar') }}
)
SELECT
	*
FROM 
      source