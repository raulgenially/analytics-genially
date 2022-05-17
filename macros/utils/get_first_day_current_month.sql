{% macro get_first_day_current_month() %}
    date_trunc(current_date(), month)
{%- endmacro -%}
