{% macro create_broad_role_field(role, broad_sector) %}
    case
        when {{ role }} = '{{ var('not_selected') }}'
            then '{{ var('not_selected') }}'
        when {{ role }} like 'Student%'
            then 
                case
                    when {{ broad_sector }} = 'K12 Education'
                        then 'Student (K12)'
                    when {{ broad_sector }} = 'Higher Education'
                        then 'Student (Higher)'
                    when {{ broad_sector }} = 'Education (Other)'
                        then 'Student (Other)'
                    when {{ broad_sector }} = 'Corporate'
                        then 'Professional'
                    when {{ broad_sector }} = '{{ var('not_selected') }}'
                        then 'Student (Not-selected sector)'
                    else null
                end
        else 'Professional'
    end
{% endmacro %}
