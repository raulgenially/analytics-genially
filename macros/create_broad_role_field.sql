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
                    when {{ broad_sector }} = 'Education (Other)' or {{ broad_sector }} = 'Corporate'
                        then 'Student (Other)'
                end
        when {{ role }} like 'Teacher%'
            then 
                case
                    when {{ broad_sector }} = 'K12 Education'
                        then 'Teacher (K12)'
                    when {{ broad_sector }} = 'Higher Education'
                        then 'Teacher (Higher)'
                    when {{ broad_sector }} = 'Education (Other)' or {{ broad_sector }} = 'Corporate'
                        then 'Teacher (Other)'
                end
        else 'Professional'
    end
{% endmacro %}
