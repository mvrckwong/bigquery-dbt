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
            {{ ref('raw_seed_regions') }}
	ORDER BY
		region_id DESC
)
SELECT
	*
FROM 
      source