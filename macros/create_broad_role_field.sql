{% macro create_broad_role_field(role, broad_sector) %}
-- The broad role not only depends on the role itself, but also on the broad sector
    case
        when {{ role }} = '{{ var('not_selected') }}' or {{ broad_sector }} = '{{ var('not_selected') }}'
            then '{{ var('not_selected') }}'
        -- First, address EDU sector
        when {{ role }} like 'Student%'
            then 
                case
                    when {{ broad_sector }} = 'K12 Education'
                        then 'Student K12'
                    when {{ broad_sector }} = 'Other Education'
                        then 'Student Other Edu'
                    when {{ broad_sector }} = 'University'
                        then 'Student Uni'
                    -- This is due to some inconsistencies
                    -- See tests/src_genially_users/assert_role_info_is_tied_to_the_expected_sector_info.sql    
                    when {{ broad_sector }} = 'Company' or {{ broad_sector }} = 'Public Admin. & NGO' or {{ broad_sector }} = 'Other Corporate'
                        then 'Student Other Edu'  
                end
        when {{ role }} like 'Teacher%'
            then 
                case
                    when {{ broad_sector }} = 'K12 Education'
                        then 'Teacher K12'
                    when {{ broad_sector }} = 'Other Education'
                        then 'Teacher Other Edu'
                    when {{ broad_sector }} = 'University'
                        then 'Teacher Uni'
                    -- This is due to some inconsistencies
                    -- See tests/src_genially_users/assert_role_info_is_tied_to_the_expected_sector_info.sql    
                    when {{ broad_sector }} = 'Company' or {{ broad_sector }} = 'Public Admin. & NGO' or {{ broad_sector }} = 'Other Corporate'
                        then 'Teacher Other Edu'  
                end
        when {{ broad_sector }} = 'K12 Education'
            then 'Pro K12'
        when {{ broad_sector }} = 'Other Education'
            then 'Pro Other Edu'
        when {{ broad_sector }} = 'University'
            then 'Pro Uni'
        -- Now CORPORATE
        when {{ role }} = 'Design' or when {{ role }} = 'Designer'
            then 'Design'
        when {{ role }} = 'Marketing/Communications' or when {{ role }} = 'Marketing and Communication'
            then 'Marketing/Communications'
        when {{ role }} = 'Training' or when {{ role }} = 'Trainer'
            then 'Training'
        else 'Other Pro'
    end
{% endmacro %}
