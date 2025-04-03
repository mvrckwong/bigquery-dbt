
{{ 
	config(
		materialized='table',
		enabled=false
	)
}}

SELECT
	user_email,
	COUNT(*) AS job_count,
	SUM(total_bytes_processed)/POWER(1024, 4) AS total_tb_processed,
	SUM(total_slot_ms)/(1000*60*60) AS total_slot_hours
FROM
	`region-us-central1`.INFORMATION_SCHEMA.JOBS
WHERE
	creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
GROUP BY
	user_email
ORDER BY
	total_slot_hours DESC