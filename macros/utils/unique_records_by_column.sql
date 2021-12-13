-- Return unique records from a table according to a specific column.
-- When ordering is specified, and several records have the same value
-- in said column, we take the first record.
--
-- For example:
-- +----+------------+----------+                                        +----+------------+----------+
-- | id |  column_a  | column_b |        unique_records_by_column        | id |  column_a  | column_b |
-- +----+-------------+---------+          cte=example,                  +----+-------------+---------+
-- |  1 | A          |        4 |    ->    unique_column=column_a,  ->   |  2 | B          |        5 |
-- |  2 | B          |        5 |          order_by=id,                  |  3 | A          |        6 |
-- |  3 | A          |        6 |          dir='desc'                    +----+------------+----------+
-- +----+------------+----------+        )

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
