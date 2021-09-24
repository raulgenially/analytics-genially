{% macro create_broad_role_field(role, broad_sector) %}
    case
        when {{ role }} = '{{ var('not_selected') }}' or {{ broad_sector }} = '{{ var('not_selected') }}'
            then '{{ var('not_selected') }}'
        when {{ role }} like 'Student%'
            then 
                case
                    when {{ broad_sector }} = 'K12 Education'
                        then 'Student (K12)'
                    when {{ broad_sector }} = 'Higher Education'
                        then 'Student (Higher)'
                    -- In the old onboarding, there can be students from the Corporate sector
                    when {{ broad_sector }} = 'Education (Other)' or {{ broad_sector }} = 'Corporate'
                        then 'Student (Other)'
                    else null
                end
        else 'Professional'
    end
{% endmacro %}
