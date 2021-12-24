{% macro get_lag_dimension_monthly_projections(metric, days) %}

lag( {{ metric }}, {{ days }} ) over (
            partition by 
                plan,
                subscription,
                country,
                country_name
            order by date_day asc
        )

{% endmacro %}