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

user_plan as (
    select * from {{ ref('user_plan_history') }}
),

last_plan as (
    {{
        unique_records_by_column(
            cte='user_plan',
            unique_column='user_id, date(started_at)',
            order_by='started_at',
            dir='desc',
            )
    }}
),

plan_dates as (
    select
        date(dates.date_day) as date_day,
        countif(last_plan.subscription = 'Free') as free_users,
        countif(last_plan.subscription = 'Premium') as premium_users

    from dates
    left join last_plan
        on date(dates.date_day) between date(last_plan.started_at) and date(last_plan.finished_at)
    group by 1
),

final as (
    select
        *,
        -- to facilite the visualization and understanding of the metric we consider premium users per 10000 free users
        safe_multiply(
            safe_divide(premium_users, free_users),
             10000) as kr

    from plan_dates
)

select * from final
