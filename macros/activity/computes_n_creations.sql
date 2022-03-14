{% macro compute_n_creations(n_days_minus) %}
    sum(n_creations) over (
        partition by user_id
        order by date_day asc
        rows between {{ n_days_minus }} preceding
            and current row
    )
{% endmacro %}
