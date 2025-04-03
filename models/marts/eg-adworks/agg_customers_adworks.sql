{{
	config(
		materialized = 'view',
		tags=[
			'adworks',
			'metrics'
		]
	)
}}


WITH agg_stats AS (
	SELECT
		COUNT(*) AS total_customers,
		AVG(annual_income) AS avg_annual_income
	FROM 
		{{ ref('dim_customers_adworks') }}
	WHERE 
		_is_current = TRUE
)

SELECT 
	* 
FROM 
	agg_stats