{{
	config(
		materialized='table'
	) 
}}

WITH source AS (
      SELECT
		*
      FROM 
            {{ ref('Regions') }}
)
SELECT
	*
FROM 
      source