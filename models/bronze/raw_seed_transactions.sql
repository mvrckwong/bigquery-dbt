{{
	config(
		materialized='table'
	) 
}}

WITH source AS (
      SELECT
		*
      FROM 
            {{ ref('Overall_Transactions') }}
)
SELECT
	*
FROM 
      source