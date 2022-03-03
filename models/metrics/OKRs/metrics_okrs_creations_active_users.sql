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

geniallys as (
    select * from {{ ref('geniallys') }}
),

monthly_active_users as (
    select
        dates.date_day as date_day,
        sum(active_users.n_monthly_active_users) as active_users

    from dates
    left join active_users
        on active_users.date_day = date(dates.date_day)
    group by 1
),

monthly_creations as (
    select
        dates.date_day as date_day,
        count(geniallys.genially_id) as creations

    from dates
    left join geniallys
        on date(dates.date_day) between date(geniallys.created_at) and date(geniallys.created_at) + 27
    group by 1
),

final as (
    select
        date(a.date_day) as date_day,
        a.active_users,
        c.creations,
        safe_divide(c.creations, a.active_users) as kr

    from monthly_active_users as a
    left join monthly_creations as c
        on a.date_day = c.date_day
)

select * from final
