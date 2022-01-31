{% set week_days = 7 %}
{% set month_days = 28 %}
{% set year_days = 364 %}

{% set min_date %}
    date('2021-12-27')
{% endset %}

with dates as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date=min_date,
        end_date="current_date()"
        )
    }}
),

-- Dimensions
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

-- Period Time
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

logins as (
    select
        user_id,
        date(login_at) as login_at

    from {{ ref('user_logins') }}
    where date(login_at) >= {{ min_date }}
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

periods_metric as (
    select
        periods.period_id,
        periods.date_day,
        periods.plan,
        logins.user_id

    from periods
    left join logins_users as logins
        on logins.login_at <= periods.period_end
            and logins.login_at > period_start
            and periods.plan = logins.plan
),

daily_metric as (
    select
        date_day,
        plan,
        count(distinct user_id) as n_logins

    from periods_metric
    where period_id = '01'
    group by 1, 2
),

weekly_metric as (
    select
        date_day,
        plan,
        count(distinct user_id) as n_logins

    from periods_metric
    where period_id = '07'
    group by 1, 2
),

monthly_metric as (
    select
        date_day,
        plan,
        count(distinct user_id) as n_logins

    from periods_metric
    where period_id = '28'
    group by 1, 2
),

final as (
    select
        -- Dimensions
        daily_metric.date_day,
        daily_metric.plan,
        
        -- Metrics
        daily_metric.n_logins as daily_logins,
        weekly_metric.n_logins as weekly_logins,
        monthly_metric.n_logins as monthly_logins

    from daily_metric
    left join weekly_metric
        on daily_metric.date_day = weekly_metric.date_day
            and daily_metric.plan = weekly_metric.plan
    left join monthly_metric
        on daily_metric.date_day = monthly_metric.date_day
            and daily_metric.plan = monthly_metric.plan
)

/*
signups as (
    select
        -- Dimensions
        date(registered_at) as registered_at,
        plan,
        subscription,
        country,
        country_name,
        -- Metrics
        count(user_id) as n_signups

    from users
    where date(registered_at) >= {{ min_date }} -- Only focus on signups from min_date on
    {{ dbt_utils.group_by(n=5) }}
),

--reference_signups
signups_joined as (
    select
        --Dimensions
        reference_table.date_day,
        reference_table.plan,
        reference_table.subscription,
        reference_table.country,
        reference_table.country_name,
        --Metrics
        coalesce(signups.n_signups, 0) as n_signups

    from reference_table
    left join signups
        on reference_table.date_day = signups.registered_at
            and reference_table.plan = signups.plan
            and reference_table.subscription = signups.subscription
            and reference_table.country = signups.country
            and reference_table.country_name = signups.country_name
),

transformed_dates as (
    select   
        *,
        date_trunc(date_day, isoweek) as date_week,
        date_trunc(date_day, month) as date_month,
        extract(day from date_day) as day_of_month,
        extract(isoweek from date_day) as week_of_year,
        extract(month from date_day) as month_of_year,
        extract(year from date_day) as year

    from signups_joined
),

transformed_dates2 as (
    select
        *,
        row_number () over (
            partition by 
                date_week,
                plan,
                subscription,
                country,
                country_name
            order by date_day asc
        ) as day_of_week

    from transformed_dates
),

metrics as (
    select
        *,
        sum(n_signups) over (
            partition by
                plan,
                subscription,
                country,
                country_name
            order by date_day asc
            rows between {{ week_days_minus }} preceding
                and current row
        ) as n_signups_status_7d,
        sum(n_signups) over (
            partition by
                date_week,
                plan,
                subscription,
                country,
                country_name
            order by day_of_week asc
            rows between unbounded preceding
                and current row
        ) as n_signups_status_7d_cumulative

    from transformed_dates2
)

/*final as (
    select
        date_day,
        day_of_month,
        date_month,
        month_of_year,
        date_quarter,
        quarter_of_year,
        year,
        n_signups,
        n_signups_cumulative,
        n_signups_previous_7d,
        n_signups_previous_28d,
        n_signups_previous_364d

    from metrics
    order by date_day asc
)*/

select * from final
