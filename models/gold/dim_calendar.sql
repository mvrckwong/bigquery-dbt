{{
	config(
		materialized='table',
		dataset='gold'
	) 
}}


WITH source AS (
	SELECT
		PARSE_DATE('%m/%d/%Y', calendar_date) AS calendar_date
	FROM 
		{{ ref('stg_seed_calendar') }}
)
SELECT
	*
FROM 
      source