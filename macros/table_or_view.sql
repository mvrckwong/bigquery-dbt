{% macro table_or_view(model_name=none, default='table') %}
  {# 
    Simple macro to choose between 'table' or 'view' materialization based on:
    1. Table row count
    2. Column width/complexity
    
    Args:
      model_name: Override for the model name (default: use current model)
      default: Default materialization if no metrics exist ('table' or 'view')
      
    Returns:
      string: Either 'table' or 'view'
  #}
  
  {% set model = model_name if model_name else this.identifier %}
  {% set view_row_threshold = var('view_row_threshold', 100000) %}
  
  {% if execute %}
    {# Check row count and column count from information schema #}
    {% set metrics_query %}
      WITH table_stats AS (
        SELECT
          t.table_name,
          t.row_count,
          COUNT(c.column_name) AS column_count,
          COUNTIF(c.data_type IN ('ARRAY', 'STRUCT', 'JSON', 'STRING') AND 
                  (c.character_maximum_length IS NULL OR c.character_maximum_length > 1000)) AS large_column_count
        FROM
          `{{ this.database }}`.`{{ this.schema }}`.INFORMATION_SCHEMA.TABLES t
        LEFT JOIN
          `{{ this.database }}`.`{{ this.schema }}`.INFORMATION_SCHEMA.COLUMNS c
          ON t.table_name = c.table_name
        WHERE
          t.table_name = '{{ model }}'
        GROUP BY
          t.table_name, t.row_count
      )
      
      SELECT
        COALESCE(row_count, 0) AS row_count,
        COALESCE(column_count, 0) AS column_count,
        COALESCE(large_column_count, 0) AS large_column_count
      FROM
        table_stats
    {% endset %}
    
    {% set results = run_query(metrics_query) %}
    
    {% if results and results|length > 0 %}
      {# We have metrics, make decision #}
      {% set row_count = results.columns['row_count'][0]|int if results.columns['row_count']|length > 0 else 0 %}
      {% set column_count = results.columns['column_count'][0]|int if results.columns['column_count']|length > 0 else 0 %}
      {% set large_column_count = results.columns['large_column_count'][0]|int if results.columns['large_column_count']|length > 0 else 0 %}
      
      {% set is_wide = column_count > 20 or large_column_count > 0 %}
      
      {% if row_count < view_row_threshold and not is_wide %}
        {# Small row count and not too wide - safe to use view #}
        {{ log("Using 'view' materialization for " ~ model ~ " (rows: " ~ row_count ~ ", columns: " ~ column_count ~ ")", info=True) }}
        {{ return('view') }}
      {% else %}
        {# Either large row count or wide - use table #}
        {{ log("Using 'table' materialization for " ~ model ~ " (rows: " ~ row_count ~ ", columns: " ~ column_count ~ ")", info=True) }}
        {{ return('table') }}
      {% endif %}
    {% else %}
      {# Check source tables for new models #}
      {% set source_query %}
        WITH model_refs AS (
          SELECT DISTINCT
            ref_node AS source_model
          FROM 
            `{{ target.database }}`.`dbt_artifacts.model_references`
          WHERE
            model_name = '{{ model }}'
        ),
        
        source_metrics AS (
          SELECT
            t.table_name,
            t.row_count,
            COUNT(c.column_name) AS column_count
          FROM
            model_refs r
          JOIN
            `{{ this.database }}`.INFORMATION_SCHEMA.TABLES t
            ON r.source_model = t.table_name
          LEFT JOIN
            `{{ this.database }}`.INFORMATION_SCHEMA.COLUMNS c
            ON t.table_name = c.table_name
          GROUP BY
            t.table_name, t.row_count
        )
        
        SELECT
          CAST(AVG(row_count) AS INT64) AS avg_source_rows,
          CAST(MAX(row_count) AS INT64) AS max_source_rows,
          CAST(AVG(column_count) AS INT64) AS avg_source_cols
        FROM
          source_metrics
      {% endset %}
      
      {% set source_results = run_query(source_query) %}
      
      {% if source_results and source_results.columns['max_source_rows']|length > 0 %}
        {# Use source metrics to make an educated guess #}
        {% set max_rows = source_results.columns['max_source_rows'][0]|int %}
        {% set avg_cols = source_results.columns['avg_source_cols'][0]|int %}
        
        {% if max_rows < view_row_threshold and avg_cols <= 20 %}
          {{ log("Using 'view' for new model " ~ model ~ " based on source metrics", info=True) }}
          {{ return('view') }}
        {% else %}
          {{ log("Using 'table' for new model " ~ model ~ " based on source metrics", info=True) }}
          {{ return('table') }}
        {% endif %}
      {% else %}
        {# No information available, use default #}
        {{ log("Using default materialization '" ~ default ~ "' for " ~ model, info=True) }}
        {{ return(default) }}
      {% endif %}
    {% endif %}
  {% else %}
    {# During parsing phase, return default #}
    {{ return(default) }}
  {% endif %}
{% endmacro %}