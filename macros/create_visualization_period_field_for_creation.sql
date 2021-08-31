{% macro create_visualization_period_field_for_creation(last_view_at, period_in_days=90) %}
    if(date({{ last_view_at }}) >= timestamp_sub(current_date(), interval {{ period_in_days }} day), true, false)
{% endmacro %}
