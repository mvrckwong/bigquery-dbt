{{
	config(
		materialized='table'
	) 
}}

WITH source AS (
      SELECT
		*
      FROM 
            {{ source('bronze', 'Regions') }}
)
SELECT
	*
FROM 
      source