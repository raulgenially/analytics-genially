{% set start_date_of_analysis %}
    '{{ var('start_date_of_analysis_OKR') }}'
{% endset %}

with dates as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date=start_date_of_analysis,
        end_date="current_date()"
        )
    }}
),

active_users as (
    select * from {{ ref('metrics_login_activity_active_users') }}
),

final as (
    select
        date(dates.date_day) as date_day,
        active_users.role,
        sum(active_users.n_returning_users_28d) as n_returning_active_users_28d

    from dates
    left join active_users
        on date(dates.date_day) = active_users.date_day
    {{ dbt_utils.group_by(n=2) }}
)

select * from final
