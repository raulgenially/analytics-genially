{% macro unique_records_by_column(cte, unique_column, order_by=null, dir='asc') %}
with numbered as (
    select
        *,
        row_number() over (
            partition by {{unique_column}}{% if order_by %} order by {{ order_by }} {{ dir }}{% endif %}
        ) as __seqnum
    from {{ cte }}
)
select *
from numbered
where __seqnum = 1
{% endmacro %}
