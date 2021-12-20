{% macro get_active_user_categories() %}
    {{ return(['New', 'Current', 'Churned']) }}
{% endmacro %}
