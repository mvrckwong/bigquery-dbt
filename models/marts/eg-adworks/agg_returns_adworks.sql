{{
	config(
		materialized = 'view',
		tags=[
			'adworks',
			'metrics'
		]
	)
}}

WITH return_metrics AS (
	SELECT
		-- Metric 1: Total return count
		count(return_date) AS total_return_count,
		
		-- Metric 2: Total return quantity
		sum(return_quantity) AS total_return_quantity,
		
		-- Metric 3: Average return quantity per return
		CASE 
			WHEN count(return_date) > 0 THEN sum(return_quantity) / count(return_date)
			ELSE 0 
		END AS avg_quantity_per_return,
		
		-- Metric 4: Count of returns by territory
		count(distinct territory_key) AS territory_return_count,
		
		-- Metric 5: Count of products returned
		count(distinct product_key) AS product_return_count
  	FROM 
		{{ ref('fact_returns_adworks') }}
)

SELECT
	'total_return_count' AS name, 
	total_return_count AS attribute
FROM 
  	return_metrics
UNION ALL
SELECT 
	'total_return_quantity' AS name, 
	total_return_quantity AS attribute 
FROM 
  	return_metrics
UNION ALL
SELECT 
	'avg_quantity_per_return' AS name, 
	avg_quantity_per_return AS attribute 
FROM 
  	return_metrics
UNION ALL
SELECT 
	'territory_return_count' AS name, 
	territory_return_count AS attribute 
FROM 
  	return_metrics
UNION ALL
SELECT 
	'product_return_count' AS name, 
	product_return_count AS attribute 
FROM 
	return_metrics