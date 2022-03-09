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

activity as (
    select * from {{ ref('metrics_login_activity_active_users') }}
),

monthly_activity as (
    select
        date(dates.date_day) as date_day,
        sum(activity.n_monthly_active_users) as active_users,
        sum(activity.n_monthly_creations) as creations

    from dates
    left join activity
        on date(dates.date_day) = activity.date_day
    group by 1
),

final as (
    select
        date_day,
        active_users,
        creations,
        safe_divide(creations, active_users) as kr

    from monthly_activity
)

select * from final
