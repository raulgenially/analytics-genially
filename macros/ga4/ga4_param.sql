{% macro ga4_param(record, key, key_type, alias=null) %}
    (
        select value.{{ key_type }}_value
        from unnest({{ record }})
        where key="{{ key }}"
    ) as {% if alias != null %}{{ alias }}{% else %}{{ key }}{% endif %}
{% endmacro %}
