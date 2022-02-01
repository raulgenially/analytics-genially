{% set week_days = 7 %}
{% set month_days = 28 %}

{% set min_date_for_computations %}
    date('2021-12-20')
{% endset %}

-- Use this to constraint the final CTE.
{% set start_date_of_analysis %}
    date('2021-12-27')
{% endset %}

-- Spine with daily granularity. Later we can sample to observe data points every week or month.
with dates as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date=min_date_for_computations,
        end_date="current_date()"
        )
    }}
),

-- For this example I am using a single dimension: Plan.
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

-- Period Time, lookup table to select whatever period time (window size) you like.
-- The period time tells you how many data points prior to the date of interest you are using for metric calculation.
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

-- Now let's select the metric of interest, in this case Logins.
users as (
    select * from {{ ref('users') }}
),

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
    inner join users -- Inner join for this analysis (we have logins of former users).
        on logins.user_id = users.user_id
),

-- So far this is the situation:
-- Metrics: Number of Logins
-- Dimensions: Plan
-- Granularity: Daily
-- Next we are selecting a weekly time period --> Weekly Logins

-- Weekly logins
metric as (
    select
        periods.date_day,
        periods.plan,
        -- The metric at date_day tells you the number of logins in the last 7 days (including logins at date_day)
        count(distinct logins.user_id) as n_weekly_logins -- Weekly Logins. 

    from periods
    left join logins_users as logins
        on logins.login_at <= periods.period_end
            and logins.login_at > period_start
            and periods.plan = logins.plan
    where period_id = '07' -- The id for selecting weekly time period.
    group by 1, 2
),

-- Period-over-period comparison. I want to compare the metric with previous 7 days.
metric_lagged as (
    select
        *,
        lag(n_weekly_logins, {{ week_days }}) over (
            partition by
                plan
            order by date_day asc
        ) n_weekly_logins_previous_7d

    from metric
),

-- So far we have considered a daily granularity, but it's straightforward to sample your data to change this.
-- Let's create some useful fields first.
transformed_dates as (
    select   
        *,
        date_trunc(date_day, isoweek) as date_week,
        row_number () over (
            partition by 
                date_trunc(date_day, isoweek),
                plan
            order by date_day asc
        ) as day_of_week

    from metric_lagged
),

-- I want weekly granularity. Therefore, I want Weekly Logins observed every week.
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

-- And this is our result:
-- Metrics: Number of Logins
-- Dimensions: Plan
-- Granularity: Weekly
-- Time Period: Weekly --> Weekly Logins

select * from final
