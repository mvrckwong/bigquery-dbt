{{
	config(
		materialized = 'view',
		tags=[
			'adworks',
			'metrics'
		]
	)
}}

WITH annual_income_data AS (
	SELECT annual_income
	FROM {{ ref('dim_customers_adworks') }}
	WHERE _is_current = TRUE
),

median_calc AS (
	SELECT APPROX_QUANTILES(annual_income, 100)[OFFSET(50)] AS median_annual_income
	FROM annual_income_data
),

customer_metrics AS (
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
UNION ALL
SELECT
	'median_annual_income' AS name,
	median_annual_income AS attribute
FROM
	median_calc