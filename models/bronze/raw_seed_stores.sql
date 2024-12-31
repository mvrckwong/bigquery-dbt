{{
	config(
		materialized='table'
	) 
}}

WITH source AS (
      SELECT
		*
      FROM 
            {{ ref('Stores') }}
)
SELECT
	*
FROM 
      source