{% macro set_country_order(country) %}
    case
        when {{ country }} = 'US'
            then '01-'
        when {{ country }} = 'FR'
            then '02-'
        when {{ country }} = 'ES'
            then '03-'
        when {{ country }} = 'BR'
            then '04-'
        when {{ country }} = 'MX'
            then '05-'
        when {{ country }} = 'CO'
            then '06-'
        when {{ country }} = 'IT'
            then '07-'
        when {{ country }} = 'UK'
            then '08-'
        when {{ country }} = 'DE'
            then '09-'
        else '10-'
    end
{% endmacro %}
