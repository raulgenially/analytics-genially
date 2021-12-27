{% macro get_license_status_values() %}
    {{ return(['Finished', 'Pending', 'Canceled']) }}
{% endmacro %}
