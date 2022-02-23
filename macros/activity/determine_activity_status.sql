{% macro determine_activity_status(min_date_logins, days, days_minus) %}
    if(
        date_diff(date_day, {{ min_date_logins }}, day) >= {{ days_minus }},
        case
            when n_days_since_first_usage < {{ days }}
                then 'New'
            when {{ compute_n_days_active(days_minus) }} = 0
                then 'Churned'
            when {{ compute_n_days_active(days_minus) }} > 0
                then 'Returning'
            else
                '{{ var('unknown') }}'
        end,
        null
    )
{% endmacro %}
