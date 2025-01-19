{{
	config(
		materialized='table'
	) 
}}

WITH source AS (
      SELECT
		*
      FROM 
            {{ ref('Customers') }}
)
SELECT
	*
FROM 
      source