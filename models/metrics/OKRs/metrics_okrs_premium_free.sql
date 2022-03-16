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
    with numbered as (
        select
            *,
            row_number() over (
                partition by user_id, date(started_at) order by started_at desc
            ) as __seqnum

        from user_plan
    )

    select *
    from numbered
    where __seqnum = 1
),

plan_dates as (
    select
        date(dates.date_day) as date_day,
        countif(subscription='Free') as f_users,
        countif(subscription='Premium') as p_users

    from dates
    left join last_plan
        on date(dates.date_day) between date(last_plan.started_at) and date(last_plan.finished_at)
    group by 1
),

final as (
    select
        *,
        -- to facilite the visualization and understanding of the metric we consider premium users per 10000 free users
        safe_divide(p_users,f_users) * 10000 as kr

    from plan_dates
)

select * from final
