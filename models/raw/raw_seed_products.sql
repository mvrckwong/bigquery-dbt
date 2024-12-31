{{
	config(
		materialized='table'
	) 
}}

WITH source AS (
      SELECT
		*
      FROM 
            {{ source('bronze', 'Products') }}
)
SELECT
	*
FROM 
      source