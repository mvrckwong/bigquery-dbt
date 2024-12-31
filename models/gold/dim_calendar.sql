{{
	config(
		materialized='table',
		dataset='silver'
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