{{
	config(
		materialized='table',
		dataset='silver'
	) 
}}

WITH source AS (
      SELECT
		*
		, CURRENT_TIMESTAMP() AS _created_date 
      FROM 
            {{ ref('raw_seed_regions') }}
	ORDER BY
		region_id DESC
)
SELECT
	*
FROM 
      source