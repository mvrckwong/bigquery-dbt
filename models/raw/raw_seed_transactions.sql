{{
	config(
		materialized='table'
	) 
}}

WITH source AS (
      SELECT
		*
      FROM 
            {{ source('bronze', 'Calendar') }}
)
SELECT
	*
FROM 
      source