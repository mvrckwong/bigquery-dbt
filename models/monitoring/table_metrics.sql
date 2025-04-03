{{ 
    config(
        materialized='table',
        enabled=false
    )
}}

SELECT
	table_schema || '.' || table_name AS table_path,
	SUM(total_logical_bytes)/POWER(1024, 3) AS total_gb,
	MAX(creation_time) AS created_at
FROM
    	`{{ target.project }}.region-us-central1.INFORMATION_SCHEMA.TABLE_STORAGE`
GROUP BY 1
ORDER BY 
	total_gb DESC