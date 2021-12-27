{% macro place_main_dimension_fields(table) %}
    {{ table }}.plan,
    {{ table }}.subscription,
    {{ table }}.sector,
    {{ table }}.broad_sector,
    {{ table }}.role,
    {{ table }}.broad_role,
    {{ table }}.country,
    {{ table }}.country_name
{% endmacro %}
