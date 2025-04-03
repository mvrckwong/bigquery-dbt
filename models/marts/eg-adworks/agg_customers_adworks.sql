{{
	config(
		materialized = 'view',
		tags=[
			'adworks',
			'metrics'
		]
	)
}}

WITH customer_metrics AS (
	SELECT
		COUNT(*) AS total_customers,
		AVG(annual_income) AS avg_annual_income
	FROM 
		{{ ref('dim_customers_adworks') }}
	WHERE
		_is_current = TRUE
)

SELECT
	'total_customers' AS name, 
	total_customers AS attribute
FROM 
	customer_metrics
UNION ALL
SELECT 
	'avg_annual_income' AS name, 
	avg_annual_income AS attribute 
FROM 
	customer_metrics