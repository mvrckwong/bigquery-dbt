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
		, IFNULL(recyclable, 0) AS is_product_recyclable
		, IFNULL(low_fat, 0) AS is_product_lowfat 
      FROM
		{{ ref('raw_seed_products') }}
	ORDER BY
		product_id DESC
)
SELECT
	*
FROM 
      source