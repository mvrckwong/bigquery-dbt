{{
	config(
		materialized='table'
	) 
}}

WITH source AS (
      SELECT
		*
      FROM 
            {{ ref('Calendar') }}
)
SELECT
	*
FROM 
      source