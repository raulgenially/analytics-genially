{% macro compute_n_days_active(n_days_minus) %}
    countif(is_active = true) over (
        partition by user_id
        order by date_day asc
        rows between {{ n_days_minus }} preceding
            and current row
    )
{% endmacro %}
