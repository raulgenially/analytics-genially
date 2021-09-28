{% macro array_from_str(column, json_path = '$') %}
    array(
        select json_extract_scalar(string_element)
        from unnest(json_extract_array({{ column }}, '{{ json_path }}')) as string_element
    )
{%- endmacro -%}
