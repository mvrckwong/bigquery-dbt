-- macros/unpivot_metrics.sql
{% macro unpivot_metrics(metrics_dict) %}
  {% for metric_name, metric_value in metrics_dict.items() %}
    select '{{ metric_name }}' as name, {{ metric_value }} as attribute
    {% if not loop.last %}union all{% endif %}
  {% endfor %}
{% endmacro %}