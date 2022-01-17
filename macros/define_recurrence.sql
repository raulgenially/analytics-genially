{% macro define_recurrence() %}
    date(last_access_at) > date(registered_at)
{% endmacro %}
