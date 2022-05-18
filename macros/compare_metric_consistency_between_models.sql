-- This macro receives a list of ctes with only one column (metric) and one row,
-- and compares if for all the ctes the value is the same
{% macro compare_metric_consistency_between_models(ctes, metric) %}    
    with

    {% for cte in ctes %}
    {{ cte }}_sum as (
        select
            sum({{ metric }}) as {{ metric }}
        from {{ cte }}
    ),
    {% endfor %}
    
    {{ metric }}_union as (
        {% for cte in ctes %}
        {% if loop.index > 1 %}union all{% endif %}
        select {{ metric }} from {{ cte }}_sum
        {% endfor %}
    ),

    {{ metric }}_count_distinct as (
        select
            count(distinct {{ metric }}) as n_distinct_values
    
        from {{ metric }}_union
    ),
    
    totals_join as (
        select
            {% for cte in ctes %}
            {{ cte }}_sum.{{ metric }} as {{ cte }}_sum_{{ metric }},
            {% endfor %}
            {{ metric }}_count_distinct.n_distinct_values
    
        from 
            {% for cte in ctes %}
            {{ cte }}_sum
            cross join
            {% endfor %}
            {{ metric }}_count_distinct
    ),

    final as (
        select * 
        from totals_join
        where n_distinct_values != 1
    )

    select * from final
		
{% endmacro %}
