{% macro clean_country_code(country_code) %}
    case
        when {{ country_code }} = 'GB'
            then 'UK'
        when {{ country_code }} = 'CO '
            then 'CO'
        when {{ country_code }} = 'HUN'
            then 'HU'
        when {{ country_code }} = 'ISRAEL'
            then 'IL'
        when {{ country_code }} = '' or {{ country_code }} is null
            then 'Not-selected'
        else {{ country_code }}
    end
{% endmacro %}
