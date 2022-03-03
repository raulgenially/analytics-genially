{% set start_date_of_analysis %}
    date('2022-01-01')
{% endset %}

with dates as (
    {{ dbt_utils.date_spine(
        datepart = "day",
        start_date = start_date_of_analysis,
        end_date = "current_date()"
        )
    }}
),

login_activity as (
    select * from {{ ref('metrics_login_activity') }}
),

oneday_users as (
    select
        date_day,
        countif(status_28d in ('New') and n_days_active_28d = 1) as n_oneday_users,
        countif(status_28d in ('New')) as n_signup_users

    from login_activity
    group by 1

),

final as (
    select
        dates.date_day as date_day,
        oneday_users.n_oneday_users as oneday_users,
        oneday_users.n_signup_users as signup_users,
        safe_divide(oneday_users.n_oneday_users,oneday_users.n_signup_users) as kr

    from dates
    left join oneday_users
    on oneday_users.date_day = date(dates.date_day)
)

select * from final
