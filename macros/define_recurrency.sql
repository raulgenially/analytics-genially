{% macro define_recurrency() %}
    date(last_access_at) > date(registered_at)
{% endmacro %}
