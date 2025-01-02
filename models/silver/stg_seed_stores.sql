{{
	config(
		materialized='table',
		dataset='silver'
	) 
}}

WITH source AS (
      SELECT
		store_id
		, region_id
		, store_type
		, store_name
		, store_street_address
		, store_city
		, store_state
		, store_country
		, store_phone
		, first_opened_date AS store_first_opened_date
		, last_remodel_date AS store_last_remodel_date
		, total_sqft AS store_total_sqft
		, grocery_sqft AS store_grocery_sqft
      FROM 
            {{ ref('raw_seed_stores') }}
	ORDER BY
		store_id ASC
)
SELECT
	*
FROM 
      source