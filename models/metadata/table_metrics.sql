{{
    config(
        materialized='table',
        schema='metadata',
        tags=['metadata']
    )
}}

WITH all_tables AS (
    SELECT
        table_catalog,
        table_schema,
        table_name,
        'TABLE' AS table_type
    FROM
        `{{ target.database }}`.INFORMATION_SCHEMA.TABLES
    WHERE
        table_type = 'BASE TABLE'
    
    UNION ALL
    
    SELECT
        table_catalog,
        table_schema,
        table_name,
        'VIEW' AS table_type
    FROM
        `{{ target.database }}`.INFORMATION_SCHEMA.VIEWS
),

table_stats AS (
    SELECT
        t.table_catalog,
        t.table_schema,
        t.table_name,
        t.table_type,
        COALESCE(s.row_count, 0) AS row_count,
        COALESCE(s.total_bytes, 0) AS total_bytes,
        COALESCE(SAFE_DIVIDE(s.total_bytes, NULLIF(s.row_count, 0)), 0) AS avg_bytes_per_row
    FROM
        all_tables t
    LEFT JOIN
        `{{ target.database }}`.INFORMATION_SCHEMA.TABLE_STORAGE s
        ON t.table_catalog = s.table_catalog
        AND t.table_schema = s.table_schema
        AND t.table_name = s.table_name
),

column_stats AS (
    SELECT
        table_catalog,
        table_schema,
        table_name,
        COUNT(*) AS column_count,
        COUNTIF(data_type IN ('ARRAY', 'STRUCT', 'JSON', 'STRING') AND 
                (character_maximum_length IS NULL OR character_maximum_length > 1000)) AS large_column_count
    FROM
        `{{ target.database }}`.INFORMATION_SCHEMA.COLUMNS
    GROUP BY
        table_catalog, table_schema, table_name
),

final AS (
    SELECT
        ts.table_catalog,
        ts.table_schema,
        ts.table_name,
        ts.table_type,
        ts.row_count,
        ts.total_bytes,
        ts.avg_bytes_per_row,
        cs.column_count,
        cs.large_column_count,
        
        -- Recommendation based on simplified rules
        CASE
            WHEN ts.row_count < {{ var('view_row_threshold', 100000) }} 
                 AND cs.column_count <= 20 
                 AND cs.large_column_count = 0 
                 THEN 'view'
            ELSE 'table'
        END AS recommended_materialization,
        
        -- Is the current materialization optimal?
        CASE
            WHEN (ts.row_count < {{ var('view_row_threshold', 100000) }} 
                  AND cs.column_count <= 20 
                  AND cs.large_column_count = 0 
                  AND ts.table_type = 'VIEW')
                 OR
                 ((ts.row_count >= {{ var('view_row_threshold', 100000) }} 
                   OR cs.column_count > 20 
                   OR cs.large_column_count > 0)
                  AND ts.table_type = 'TABLE')
                 THEN TRUE
            ELSE FALSE
        END AS is_optimal_materialization,
        
        CURRENT_TIMESTAMP() AS analyzed_at
    FROM
        table_stats ts
    LEFT JOIN
        column_stats cs
        ON ts.table_catalog = cs.table_catalog
        AND ts.table_schema = cs.table_schema
        AND ts.table_name = cs.table_name
)

SELECT * FROM final