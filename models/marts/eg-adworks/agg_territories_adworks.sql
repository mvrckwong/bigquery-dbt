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
		-- Region diversity score (number of unique regions)
		COUNT(DISTINCT region) AS unique_regions_count,
		
		-- Country diversity score (number of unique countries)
		COUNT(DISTINCT country) AS unique_countries_count,
		
		-- Continent diversity score (number of unique continents)
		COUNT(DISTINCT continent) AS unique_continents_count
	FROM 
		{{ ref('dim_territory_adworks') }}
	WHERE
		_is_current = TRUE
)

SELECT
	*
FROM
	agg_stats