{{
	config(
		materialized='incremental',
		incremental_strategy='merge',
		unique_key='ProductKey',
		on_schema_change='sync_all_columns',
		tags=['eg'],
		enabled=false
	)
}}

WITH source AS (
	SELECT
		*
	FROM 
		{{ ref('raw_products_adworks') }}
    
	{% if is_incremental() %}
	WHERE
		-- This assumes your source data has an updated_at or similar field
		-- Change the field name to match your actual data
		-- updated_at > (
		--     SELECT MAX(updated_at) 
		--     FROM {{ this }}
		-- )
		
		-- Alternatively, if you don't have an updated_at field:
		ProductKey NOT IN (
			SELECT ProductKey 
			FROM {{ this }}
		)
	{% endif %}
),

transformed_source AS (
	SELECT 
		*
	FROM 
		source
)

SELECT 
    	* 
FROM 
    	transformed_source