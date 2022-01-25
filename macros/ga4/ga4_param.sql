{% macro ga4_param(field, key, key_type, alias=null) %}
    (
        select value.{{ key_type }}_value
        from unnest({{ field }})
        where key="{{ key }}"
    ) as {% if alias != null %}{{ alias }}{% else %}{{ key }}{% endif %}
{% endmacro %}
