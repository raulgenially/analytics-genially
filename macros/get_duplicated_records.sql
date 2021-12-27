-- This macro checks if there are duplicated values in a certain column of table source
-- Discard null values
{% macro get_duplicated_records(table_source, column_name) %}

with final as (
    select
        {{ column_name }},
        count({{ column_name }}) as n_records

    from {{ table_source }}
    where {{ column_name }} is not null
    group by {{ column_name }}
    having n_records > 1
)

select * from final

{% endmacro %}
