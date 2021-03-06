{% macro extract_user_profile(profile_column, profile_name) %}
-- profile_column can be newsector or newrole
-- profile_name refers to the normalized name of the corresponding profile
    case
        when {{ profile_column }} is null or {{ profile_name }} is null
            then '{{ var('not_selected') }}'
        else {{ profile_name }}
    end
{% endmacro %}
