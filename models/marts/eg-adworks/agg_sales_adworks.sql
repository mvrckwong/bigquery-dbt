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
		-- Basic sales metrics
		COUNT(DISTINCT order_number) AS total_orders,
		SUM(order_quantity) AS total_order_quantity,
		AVG(order_quantity) AS avg_order_quantity,
		
		-- Time-based metrics
		AVG(lead_time_days) AS avg_lead_time_days,
		MAX(lead_time_days) AS max_lead_time_days,
		MIN(lead_time_days) AS min_lead_time_days,
		
		-- Fulfillment metrics
		COUNT(CASE WHEN fulfillment_category = 'Quick' THEN 1 END) AS quick_fulfillment_count,
		COUNT(CASE WHEN fulfillment_category = 'Normal' THEN 1 END) AS normal_fulfillment_count,
		COUNT(CASE WHEN fulfillment_category = 'Extended' THEN 1 END) AS extended_fulfillment_count,
		
		-- Fulfillment percentages
		COUNT(CASE WHEN fulfillment_category = 'Quick' THEN 1 END) / COUNT(*) * 100 AS quick_fulfillment_percentage,
		COUNT(CASE WHEN fulfillment_category = 'Normal' THEN 1 END) / COUNT(*) * 100 AS normal_fulfillment_percentage,
		COUNT(CASE WHEN fulfillment_category = 'Extended' THEN 1 END) / COUNT(*) * 100 AS extended_fulfillment_percentage,
	FROM 
		{{ ref('fact_sales_adworks') }}
)

SELECT
	'total_orders' AS name, 
	total_orders AS attribute
FROM 
	agg_stats
UNION ALL
SELECT 
	'total_order_quantity' AS name, 
	total_order_quantity AS attribute 
FROM 
	agg_stats
UNION ALL
SELECT 
	'avg_order_quantity' AS name, 
	avg_order_quantity AS attribute 
FROM 
	agg_stats
UNION ALL
SELECT 
	'avg_lead_time_days' AS name, 
	avg_lead_time_days AS attribute 
FROM 
	agg_stats
UNION ALL
SELECT 
	'max_lead_time_days' AS name, 
	max_lead_time_days AS attribute 
FROM 
	agg_stats
UNION ALL
SELECT 
	'min_lead_time_days' AS name, 
	min_lead_time_days AS attribute 
FROM 
	agg_stats
UNION ALL
SELECT 
	'quick_fulfillment_count' AS name, 
	quick_fulfillment_count AS attribute 
FROM 
	agg_stats
UNION ALL
SELECT 
	'normal_fulfillment_count' AS name, 
	normal_fulfillment_count AS attribute 
FROM 
	agg_stats
UNION ALL
SELECT 
	'extended_fulfillment_count' AS name, 
	extended_fulfillment_count AS attribute 
FROM 
	agg_stats
UNION ALL
SELECT 
	'quick_fulfillment_percentage' AS name, 
	quick_fulfillment_percentage AS attribute 
FROM 
	agg_stats
UNION ALL
SELECT 
	'normal_fulfillment_percentage' AS name, 
	normal_fulfillment_percentage AS attribute 
FROM 
	agg_stats
UNION ALL
SELECT 
	'extended_fulfillment_percentage' AS name, 
	extended_fulfillment_percentage AS attribute 
FROM 
	agg_stats
UNION ALL


SELECT 
	'unique_customers' AS name, 
	unique_customers AS attribute 
FROM 
	agg_stats
UNION ALL
SELECT 
	'avg_orders_per_customer' AS name, 
	avg_orders_per_customer AS attribute 
FROM 
	agg_stats
UNION ALL
SELECT 
	'unique_products' AS name, 
	unique_products AS attribute 
FROM 
	agg_stats
UNION ALL
SELECT 
	'unique_territories' AS name, 
	unique_territories AS attribute 
FROM 
	agg_stats