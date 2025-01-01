{{
	config(
		materialized='table',
		dataset='silver'
	) 
}}

WITH source AS (
      SELECT
		product_id
		, product_brand
		, product_name
		, product_sku
		, product_retail_price
		, product_cost
		, product_weight
		, recyclable AS is_product_recyclable
		, low_fat AS is_product_low_fat
      FROM 
            {{ ref('raw_seed_products') }}
	ORDER BY
		product_id DESC
)
SELECT
	*
FROM 
      source