{{
	config(
		materialized = 'view',
		tags=[
			'adworks',
			'agg'
		]
	)
}}

WITH agg_stats AS (
	SELECT
		-- Product Count Metrics
		COUNT(DISTINCT product_key) AS count_products,
		COUNT(DISTINCT product_brand) AS count_brands,
		COUNT(DISTINCT product_color) AS count_colors,
		
		-- Product Pricing Metrics
		AVG(product_price) AS avg_product_price,
		MIN(product_price) AS min_product_price,
		MAX(product_price) AS max_product_price,
		
		-- Product Margin Metrics
		AVG(product_margin) AS avg_product_margin,
		AVG(product_margin_pct) AS avg_margin_percentage,
		COUNT(CASE WHEN product_margin_pct < 20 THEN 1 END) AS count_low_margin_products
	FROM 
		{{ ref('dim_products_adworks') }}
	WHERE
		_is_current = TRUE
)

SELECT
	'count_products' AS name, 
	count_products AS attribute
FROM 
	agg_stats
UNION ALL
SELECT 
	'count_brands' AS name, 
	count_brands AS attribute 
FROM 
	agg_stats
UNION ALL
SELECT 
	'count_colors' AS name, 
	count_colors AS attribute 
FROM 
	agg_stats
UNION ALL
SELECT
	'avg_product_price' AS name, 
	avg_product_price AS attribute
FROM 
	agg_stats
UNION ALL
SELECT 
	'min_product_price' AS name, 
	min_product_price AS attribute 
FROM 
	agg_stats
UNION ALL
SELECT 
	'max_product_price' AS name, 
	max_product_price AS attribute 
FROM 
	agg_stats
UNION ALL
SELECT
	'avg_product_margin' AS name, 
	avg_product_margin AS attribute
FROM 
	agg_stats
UNION ALL
SELECT 
	'avg_margin_percentage' AS name, 
	avg_margin_percentage AS attribute 
FROM 
	agg_stats
UNION ALL
SELECT 
	'count_low_margin_products' AS name, 
	count_low_margin_products AS attribute 
FROM 
	agg_stats