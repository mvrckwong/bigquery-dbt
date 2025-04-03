{{
	config(
		materialized = 'view',
		tags=[
			'adworks',
			'metrics'
		]
	)
}}

WITH territory_distribution AS (
	SELECT
		-- Metric 3: Region diversity score (number of unique regions)
		COUNT(DISTINCT region) AS unique_regions_count,
		
		-- Metric 4: Country diversity score (number of unique countries)
		COUNT(DISTINCT country) AS unique_countries_count,
		
		-- Metric 5: Continent diversity score (number of unique continents)
		COUNT(DISTINCT continent) AS unique_continents_count
	FROM 
		{{ ref('dim_territory_adworks') }}
	WHERE
		_is_current = TRUE
)

SELECT 
	'unique_regions_count' AS name, 
	unique_regions_count AS attribute 
FROM 
	territory_distribution
UNION ALL
SELECT 
	'unique_countries_count' AS name, 
	unique_countries_count AS attribute 
FROM 
	territory_distribution
UNION ALL
SELECT 
	'unique_continents_count' AS name, 
	unique_continents_count AS attribute 
FROM 
	territory_distribution