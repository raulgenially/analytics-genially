{% set week_days = 7 %}
{% set month_days = 28 %}

{% set min_date_for_computations %}
    date('2021-12-20')
{% endset %}

{% set start_date_of_analysis %}
    date('2021-12-27')
{% endset %}

with dates as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date=min_date_for_computations,
        end_date="current_date()"
        )
    }}
),

-- Dimensions: Plan
plans as (
    select * from {{ ref('seed_plan') }}
),

dates_plan as (
    select
        date(dates.date_day) as date_day,
        plans.plan,

    from dates
    cross join plans
),

-- Period Time, lookup table to select whatever period time you like
periods as (
    select
        '01' as period_id, -- Daily (e.g., daily active users)
        date(date_day) as date_day,
        plan,
        date(date_day) as period_end,
        date(datetime_sub(date_day, interval 1 day)) as period_start

    from dates_plan

    union all

    select
        '07' as period_id, -- Weekly (e.g., weekly active users)
        date(date_day) as date_day,
        plan,
        date(date_day) as period_end,
        date(datetime_sub(date_day, interval {{ week_days }} day)) as period_start

    from dates_plan

    union all

    select
        '28' as period_id, -- Monthly (e.g., monthly active users)
        date(date_day) as date_day,
        plan,
        date(date_day) as period_end,
        date(datetime_sub(date_day, interval {{ month_days }} day)) as period_start

    from dates_plan
),

users as (
    select * from {{ ref('users') }}
),

-- Metric: logins
logins as (
    select
        user_id,
        date(login_at) as login_at

    from {{ ref('user_logins') }}
),

logins_users as (
    select
        logins.user_id,
        logins.login_at,
        users.plan

    from logins
    left join users
        on logins.user_id = users.user_id
),

-- Weekly logins
metric as (
    select
        periods.date_day,
        periods.plan,
        count(distinct user_id) as n_weekly_logins

    from periods
    left join logins_users as logins
        on logins.login_at <= periods.period_end
            and logins.login_at > period_start
            and periods.plan = logins.plan
    where period_id = '07'
    group by 1, 2
),

-- Now let's work out the granularity
transformed_dates as (
    select   
        *,
        date_trunc(date_day, isoweek) as date_week,
        row_number () over (
            partition by 
                date_trunc(date_day, isoweek),
                plan
            order by date_day asc
        ) as day_of_week,
        lag(n_weekly_logins, 7) over (
            partition by
                plan
            order by date_day asc
        ) n_weekly_logins_previous_7d

    from metric
),

-- I want weekly granularity. Therefore, I want weekly logins observed every week
final as (
    select
        date_week,
        plan,
        n_weekly_logins,
        n_weekly_logins_previous_7d

    from transformed_dates
    where date_day >= {{ start_date_of_analysis }}
        and day_of_week = 7 -- Sampling
)

select * from final
