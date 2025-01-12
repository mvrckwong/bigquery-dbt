{{
	config(
		materialized='table',
		dataset='silver'
	) 
}}

WITH source AS (
	SELECT
		*
		, CURRENT_TIMESTAMP() AS created_at 
	FROM 
		{{ ref('raw_seed_returns') }}
	ORDER BY
		return_date ASC
)
SELECT
	*
FROM 
      source