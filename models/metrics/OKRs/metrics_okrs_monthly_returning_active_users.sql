{% set start_date_of_analysis %}
    date('2022-01-01')
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
        sum(active_users.n_returning_users_28d) as n_returning_active_users_28d

    from dates
    left join active_users
        on date(dates.date_day) = active_users.date_day
    group by 1
)

select * from final
