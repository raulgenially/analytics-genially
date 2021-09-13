{% macro define_recurrent_publisher() %}
    {{ define_publisher() }}
    and date(last_access_at) > date(registered_at)
{% endmacro %}
