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
            {{ ref('raw_seed_returns') }}
)
SELECT
	*
FROM 
      source