{% macro get_lag_dimension_metrics_reporting(metric, period, date_part) %}

lag( {{ metric }}, {{ period }} ) over (
            partition by
                plan,
                subscription,
                country,
                country_name,
                broad_sector,
                broad_role
            order by {{ date_part }} asc
        )

{% endmacro %}
