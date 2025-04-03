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
		count(return_date) AS count_returns,
		sum(return_quantity) AS sum_return_quantity
	FROM 
		{{ ref('fact_returns_adworks') }}
)

SELECT
	'count_returns' AS name, 
	count_returns AS attribute
FROM 
	agg_stats
UNION ALL
SELECT 
	'sum_return_quantity' AS name, 
	sum_return_quantity AS attribute 
FROM 
	agg_stats