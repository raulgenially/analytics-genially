{% set min_date_for_computations %}
    date('2021-12-20')
{% endset %}

{% set max_date_for_computations %}
    date('2023-01-01')
{% endset %}

{% set start_date_of_analysis %}
    date('2022-01-01')
{% endset %}

with dates as (
    {{ dbt_utils.date_spine(
        datepart = "day",
        start_date = min_date_for_computations,
        end_date = max_date_for_computations
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
    on date(oneday_users.date_day) = dates.date_day
    where dates.date_day >= {{ start_date_of_analysis }}    
)

select * from final
