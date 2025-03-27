{% macro smart_materialization(model_name=none, default='table') %}
  {# 
    Determines the best materialization strategy based on:
    1. Existing metrics from prior runs
    2. Source table sizes (for models with a single source)
    3. Default fallback for new models
    
    Args:
      model_name: Override for the model name (default: use current model)
      default: Default materialization if no metrics exist ('view', 'table', 'incremental')
      
    Returns:
      string: Recommended materialization strategy
  #}
  
  {% set model = model_name if model_name else this.identifier %}
  
  {% if execute %}
    {# Check if we have metrics for this model from previous runs #}
    {% set metrics_query %}
      SELECT
        recommended_materialization
      FROM
        `{{ target.database }}.metadata.table_metrics`
      WHERE
        table_name = '{{ model }}'
      ORDER BY
        last_updated DESC
      LIMIT 1
    {% endset %}
    
    {% set results = run_query(metrics_query) %}
    
    {% if results and results.columns['recommended_materialization'] | length > 0 %}
      {# We have metrics, use the recommended materialization #}
      {% set materialization = results.columns['recommended_materialization'][0] %}
      {{ log("Using recommended materialization '" ~ materialization ~ "' for " ~ model ~ " based on metrics", info=True) }}
      {{ return(materialization) }}
      
    {% else %}
      {# No metrics yet, try to make an educated guess based on source tables #}
      {% set source_query %}
        WITH model_deps AS (
          SELECT
            DISTINCT downstream_node AS source_model
          FROM
            `{{ target.database }}.dbt_artifacts.model_dependencies`
          WHERE
            node = '{{ model }}'
            AND downstream_node != '{{ model }}'
        ),
        
        source_metrics AS (
          SELECT
            m.*
          FROM
            `{{ target.database }}.metadata.table_metrics` m
          JOIN
            model_deps d
            ON m.table_name = d.source_model
        )
        
        SELECT
          AVG(row_count) AS avg_source_rows,
          MAX(row_count) AS max_source_rows,
          AVG(column_count) AS avg_source_cols,
          COUNT(*) AS source_count
        FROM
          source_metrics
      {% endset %}
      
      {% set source_results = run_query(source_query) %}
      
      {% if source_results and source_results.columns['max_source_rows'] | length > 0 %}
        {# We have source metrics, make an educated guess #}
        {% set max_rows = source_results.columns['max_source_rows'][0] %}
        {% set avg_cols = source_results.columns['avg_source_cols'][0] %}
        {% set source_count = source_results.columns['source_count'][0] %}
        
        {% set is_wide = avg_cols > 20 %}
        
        {% if source_count == 1 %}
          {# Direct pass-through with transformations #}
          {% if max_rows < var('small_table_threshold', 100000) and not is_wide %}
            {{ log("Using 'view' for " ~ model ~ " based on small source table", info=True) }}
            {{ return('view') }}
          {% elif max_rows < var('medium_table_threshold', 10000000) %}
            {{ log("Using 'table' for " ~ model ~ " based on medium source table", info=True) }}
            {{ return('table') }}
          {% else %}
            {{ log("Using 'incremental' for " ~ model ~ " based on large source table", info=True) }}
            {{ return('incremental') }}
          {% endif %}
        {% else %}
          {# Multiple sources being joined #}
          {% if max_rows < var('small_table_threshold', 100000) / 10 %}
            {# Even with joins, should be small #}
            {{ log("Using 'tablea' for " ~ model ~ " based on small joined sources", info=True) }}
            {{ return('table') }}
          {% else %}
            {# Joins on larger tables should be incremental #}
            {{ log("Using 'incremental' for " ~ model ~ " based on joined large sources", info=True) }}
            {{ return('incremental') }}
          {% endif %}
        {% endif %}
      {% else %}
        {# No source metrics either, use default #}
        {{ log("Using default materialization '" ~ default ~ "' for " ~ model, info=True) }}
        {{ return(default) }}
      {% endif %}
    {% endif %}
  {% else %}
    {# During parsing phase, return default #}
    {{ return(default) }}
  {% endif %}
{% endmacro %}