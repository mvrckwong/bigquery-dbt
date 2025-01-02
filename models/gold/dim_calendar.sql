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
	ORDER BY
		calendar_date ASC
)
SELECT
	calendar_date
	, EXTRACT(DAYOFWEEK FROM calendar_date) AS day_of_week
	, EXTRACT(DAY FROM calendar_date) AS day
	, EXTRACT(DAYOFYEAR FROM calendar_date) AS day_of_year
	, EXTRACT(WEEK FROM calendar_date) AS week
	, EXTRACT(ISOWEEK FROM calendar_date) AS iso_week
	, EXTRACT(MONTH FROM calendar_date) AS month
	, EXTRACT(QUARTER FROM calendar_date) AS quarter
	, EXTRACT(YEAR FROM calendar_date) AS year
	, EXTRACT(ISOYEAR FROM calendar_date) AS iso_year
	, CURRENT_DATE AS current_date
FROM 
      source