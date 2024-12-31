{{
	config(
		materialized='table',
		dataset='gold'
	) 
}}

WITH source AS (
      SELECT
		*
      FROM 
            {{ ref('stg_seed_calendar') }}
)
SELECT
	*
FROM 
      source