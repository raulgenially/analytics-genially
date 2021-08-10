{% macro map_old_sector_category(category) %}
    case
        when {{ category }} = 'EDU'
            then 'Education'
        when {{ category }} = 'CORPORATE'
            then 'Corporate'
        else {{ category }}
    end
{% endmacro %}
