{% macro get_lag_dimension_monthly_projections(metric, days, date_field) %}

lag( {{ metric }}, {{ days }} ) over (
            partition by
                plan,
                subscription,
                country,
                country_name,
                broad_sector,
                broad_role
            order by {{ date_field }} asc
        )

{% endmacro %}
