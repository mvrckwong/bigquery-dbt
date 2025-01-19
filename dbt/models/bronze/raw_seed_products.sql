{{
	config(
		materialized='table'
	) 
}}

WITH source AS (
      SELECT
		*
      FROM 
            {{ ref('Products') }}
)
SELECT
	*
FROM 
      source