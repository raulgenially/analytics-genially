{% macro define_active_creation(table) %}
    {{ table }}.is_deleted = false
{% endmacro %}
