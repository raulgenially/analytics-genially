{% macro get_query_source_values() %}
    {{ return(['dbt', 'Metabase', 'Data Studio', 'Other', 'Hevo']) }}
{% endmacro %}
