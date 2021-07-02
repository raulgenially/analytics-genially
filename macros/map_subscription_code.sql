{% macro map_subscription_code(subscription_code) %}
    case 
        when {{ subscription_code }} = 1
            then 'Free'
        when {{ subscription_code }} = 2
            then 'Pro'
        when {{ subscription_code }} = 3
            then 'Master'
        when {{ subscription_code }} = 4
            then 'Edu Pro'
        when {{ subscription_code }} = 5
            then 'Edu Team'
        when {{ subscription_code }} = 6
            then 'Team'
        when {{ subscription_code }} = 7
            then 'Student'
    end
{% endmacro %}