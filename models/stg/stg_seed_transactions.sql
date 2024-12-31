{{
	config(
		materialized='table'
	) 
}}

WITH source AS (
      SELECT
		*
      FROM 
            {{ source('bronze', 'Overall_Transactions') }}
)
SELECT
	*
FROM 
      source