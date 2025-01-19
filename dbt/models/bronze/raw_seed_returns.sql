{{
	config(
		materialized='table'
	) 
}}

WITH source AS (
      SELECT
		*
      FROM 
            {{ ref('Returns') }}
)
SELECT
	*
FROM 
      source