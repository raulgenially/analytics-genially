{% set start_date_of_analysis %}
    '{{ var('snapshot_users_start_date') }}'
{% endset %}

with dates as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date=start_date_of_analysis,
        end_date="current_date()"
        )
    }}
),

user_plan as (
    select * from {{ ref('user_plan_history') }}
),

final as (
    select
        date(dates.date_day) as date_day,
        countif(user_plan.subscription = 'Free') as free_users,
        countif(user_plan.subscription = 'Premium') as premium_users

    from dates
    left join user_plan
        on date(dates.date_day) between date(user_plan.started_at) and
        if (
             format_datetime("%H:%M:%S", user_plan.finished_at) = "23:59:59",
             date(user_plan.finished_at),
             date(user_plan.finished_at) - 1
        )
    group by 1
)

select * from final
