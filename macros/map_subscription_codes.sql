{% macro map_subscription_code(column_name) %}
    case 
        when {{ column_name }} = 1
            then 'Free'
        when {{ column_name }} = 2
            then 'Pro'
        when {{ column_name }} = 3
            then 'Master'
        when {{ column_name }} = 4
            then 'Edu Pro'
        when {{ column_name }} = 5
            then 'Edu Team'
        when {{ column_name }} = 6
            then 'Team'
        when {{ column_name }} = 7
            then 'Student'
    end
{% endmacro %}