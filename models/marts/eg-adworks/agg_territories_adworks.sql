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
		-- Metric 1: Percentage distribution by territory category
		ROUND(COUNT(CASE WHEN territory_category = 'Core' THEN 1 END) * 100.0 / COUNT(*), 2) AS pct_core_territories,
		ROUND(COUNT(CASE WHEN territory_category = 'Expansion' THEN 1 END) * 100.0 / COUNT(*), 2) AS pct_expansion_territories,
		
		-- Metric 2: Percentage distribution by market type
		ROUND(COUNT(CASE WHEN market_type = 'Domestic' THEN 1 END) * 100.0 / COUNT(*), 2) AS pct_domestic_territories,
		ROUND(COUNT(CASE WHEN market_type = 'International' THEN 1 END) * 100.0 / COUNT(*), 2) AS pct_international_territories,
		
		-- Metric 3: Region diversity score (number of unique regions)
		COUNT(DISTINCT region) AS unique_regions_count,
		
		-- Metric 4: Country diversity score (number of unique countries)
		COUNT(DISTINCT country) AS unique_countries_count,
		
		-- Metric 5: Continent diversity score (number of unique continents)
		COUNT(DISTINCT continent) AS unique_continents_count
	FROM 
		{{ ref('dim_territories_adworks') }}
	WHERE
		_is_current = TRUE
)

-- Format the output as name-attribute pairs to match the reference format
SELECT
	'pct_core_territories' AS name, 
	pct_core_territories AS attribute
FROM 
	territory_distribution
UNION ALL
SELECT 
	'pct_expansion_territories' AS name, 
	pct_expansion_territories AS attribute 
FROM 
	territory_distribution
UNION ALL
SELECT 
	'pct_domestic_territories' AS name, 
	pct_domestic_territories AS attribute 
FROM 
	territory_distribution
UNION ALL
SELECT 
	'pct_international_territories' AS name, 
	pct_international_territories AS attribute 
FROM 
	territory_distribution
UNION ALL
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