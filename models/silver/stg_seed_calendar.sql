{{
	config(
		materialized='table',
		dataset='silver'
	) 
}}

WITH source AS (
      SELECT
		date AS calendar_date
      FROM 
            {{ ref('raw_seed_calendar') }}
)
SELECT
	*
FROM 
      source