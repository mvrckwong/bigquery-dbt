{{
	config(
		materialized='table'
	) 
}}

WITH source AS (
      SELECT
		*
      FROM 
            {{ source('bronze', 'raw_seed_products') }}
)
SELECT
	*
FROM 
      source