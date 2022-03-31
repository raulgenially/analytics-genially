{% macro create_subscription_field(plan) %}
    case
        when {{ plan }} is null
            then '{{ var('unknown') }}'
        when {{ plan }} = 'Free'
            then 'Free'
        else 'Premium'
    end
{% endmacro %}
