{% macro compute_n_days_active(n_days_minus) %}
    countif(is_active = true) over (
        partition by user_id
        order by n_days_since_first_usage asc
        rows between {{ n_days_minus }} preceding
            and current row
    )
{% endmacro %}
