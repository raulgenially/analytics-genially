{{
  config(
    materialized='view'
  )
}}

{% set week_days = 7 %}
{% set week_days_minus = week_days - 1 %}

{% set month_days = 28 %}
{% set month_days_minus = month_days - 1 %}

{% set min_date_logins %}
    date('2021-12-20')
{% endset %}

with logins as (
    select * from {{ ref('user_logins') }}
),

users as (
    select * from {{ ref('users') }}
),

ga_signups as (
    select * from {{ ref('signup_events') }}
),

dates as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date=min_date_logins,
        end_date="current_date()"
       )
    }}
),

user_usage as (
    select
        users.user_id,
        {{ place_main_dimension_fields('users') }},
        coalesce(ga_signups.device, '{{ var('unknown') }}') as device,
        coalesce(ga_signups.channel, '{{ var('unknown') }}') as channel,
        date(users.registered_at) as first_usage_at

    from users
    left join ga_signups
        on users.user_id = ga_signups.user_id
),

user_day as (
    select
        user_usage.user_id,
        {{ place_main_dimension_fields('user_usage') }},
        user_usage.device,
        user_usage.channel,
        user_usage.first_usage_at,
        date(dates.date_day) as date_day,
        date_diff(dates.date_day, user_usage.first_usage_at, day) as n_days_since_first_usage

    from user_usage
    cross join dates
    where date(dates.date_day) >= user_usage.first_usage_at
),

user_day_traffic as (
    select
        user_day.user_id,
        {{ place_main_dimension_fields('user_day') }},
        user_day.device,
        user_day.channel,
        user_day.first_usage_at,
        user_day.date_day,
        user_day.n_days_since_first_usage,
        (user_day.n_days_since_first_usage = 0 or logins.user_id is not null) as is_active,
        case
            when user_day.n_days_since_first_usage = 0
                then 'New'
            when user_day.n_days_since_first_usage > 0 and logins.user_id is null
                then 'Churned'
            when user_day.n_days_since_first_usage > 0 and logins.user_id is not null
                then 'Current'
        end as status

    from user_day
    left join logins
        on user_day.user_id = logins.user_id
            and user_day.date_day = date(logins.login_at)
),

user_traffic_rolling_status as (
    select
        user_id,
        {{ place_main_dimension_fields('user_day_traffic') }},
        device,
        channel,
        first_usage_at,
        date_day,
        n_days_since_first_usage,
        is_active,
        status,
        {{ compute_n_days_active(week_days_minus) }} as n_days_active_7d,
        {{ compute_n_days_active(month_days_minus) }} as n_days_active_28d,
        case
            when n_days_since_first_usage < {{ week_days }}
                then 'New'
            when {{ compute_n_days_active(week_days_minus) }} = 0
                then 'Churned'
            else 'Current'
        end as status_7d,
        case
            when n_days_since_first_usage < {{ month_days }}
                then 'New'
            when {{ compute_n_days_active(month_days_minus) }} = 0
                then 'Churned'
            else 'Current'
        end as status_28d

    from user_day_traffic
),

final as (
    select
        {{ dbt_utils.surrogate_key(['user_id', 'date_day']) }} as id,
        user_id,
        {{ place_main_dimension_fields('user_traffic_rolling_status') }},
        device as signup_device,
        channel as signup_channel,
        first_usage_at,
        date_day,
        n_days_since_first_usage,
        is_active,
        status,
        n_days_active_7d,
        status_7d,
        n_days_active_28d,
        status_28d,
        lag(status, 1) over (
            partition by user_id
            order by n_days_since_first_usage asc
        ) as previous_status,
        lag(status_7d, {{ week_days }}) over (
            partition by user_id
            order by n_days_since_first_usage asc
        ) as previous_status_7d,
        lag(status_28d, {{ month_days }}) over (
            partition by user_id
            order by n_days_since_first_usage asc
        ) as previous_status_28d

    from user_traffic_rolling_status
)

select * from final
