{% macro compare_metric_consistency_between_models(ctes, metric, date_column) %}
    with

    {% for cte in ctes %}
        {{ cte }}_sum as (
            select
                {{ date_column }},
                sum({{ metric }}) as {{ metric }}
            from {{ cte }}
            group by {{ date_column }}
        ),
    {% endfor %}

    {{ metric }}_union as (
        {% for cte in ctes %}
            {% if loop.index > 1 %}union all{% endif %}
            select
                {{ date_column }},
                {{ metric }}
            from {{ cte }}_sum
        {% endfor %}
    ),

    {{ metric }}_count_distinct as (
        select
            {{ date_column }},
            count(distinct {{ metric }}) as n_distinct_values

        from {{ metric }}_union
        group by {{ date_column }}
    ),

    totals_join as (
        select
            {% for cte in ctes %}
                {{ cte }}_sum.{{ metric }} as {{ cte }}_sum_{{ metric }},
            {% endfor %}
            {{ metric }}_count_distinct.n_distinct_values,
            {{ metric }}_count_distinct.{{ date_column }}

        from
            {% for cte in ctes %}
                {% if loop.index == 1 %}{{ cte }}_sum{% endif %}
                full join
                {% if not loop.last %}
                    {{ loop.nextitem }}_sum
                        on {{ cte }}_sum.{{ date_column }} =
                            {{ loop.nextitem }}_sum.{{ date_column }}
                {% else %}
                    {{ metric }}_count_distinct
                        on {{ cte }}_sum.{{ date_column }} =
                            {{ metric }}_count_distinct.{{ date_column }}
                {% endif %}
            {% endfor %}
    ),

    final as (
        select *
        from totals_join
        where n_distinct_values != 1
    )

    select * from final

{% endmacro %}
