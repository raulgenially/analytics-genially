{% macro set_country_order(country) %}
    case
        when {{ country }} = 'US'
            then 1
        when {{ country }} = 'FR'
            then 2
        when {{ country }} = 'ES'
            then 3
        when {{ country }} = 'BR'
            then 4
        when {{ country }} = 'MX'
            then 5
        when {{ country }} = 'CO'
            then 6
        when {{ country }} = 'IT'
            then 7
        when {{ country }} = 'UK'
            then 8
        when {{ country }} = 'DE'
            then 9
        else 10
    end
{% endmacro %}
