{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}

{% set week_days = 7 %}
{% set week_days_minus = week_days - 1 %}

{% set month_days = 28 %}
{% set month_days_minus = month_days - 1 %}

{% set min_date %}
    date('2019-01-01')
{% endset %}

{% set lookback_date %}
    date_sub(current_date(), interval 56 day) -- 28 days * 2
{% endset %}

with geniallys as (
    select * from {{ ref('geniallys') }}
),

user_usage as (
   select user_id,
          min(date(created_at)) as first_usage
   from geniallys
   where created_at is not null
   group by 1
),

dates as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date=lookback_date if is_incremental() else min_date,
        end_date="current_date()"
       )
    }}
),

user_day as (
    select
        user_usage.user_id,
        user_usage.first_usage,
        dates.date_day,
        date_diff(dates.date_day, user_usage.first_usage, day) as day_period

    from user_usage
        cross join dates
    where dates.date_day >= user_usage.first_usage
        and dates.date_day < current_date()
        and user_usage.first_usage >= {{ min_date }}
),

user_day_traffic as (
    select
        distinct user_day.user_id,
        user_day.first_usage,
        user_day.date_day,
        user_day.day_period,
        max(geniallys.user_id is not null) as was_active,
        case
            when user_day.day_period = 0 then 'N'
            when user_day.day_period > 0 and max(geniallys.user_id is not null) = false then 'C'
            when user_day.day_period > 0 and max(geniallys.user_id is not null) = true then 'A'
        end as status

    from user_day
    left join geniallys
        on user_day.user_id = geniallys.user_id
        and user_day.date_day = date(geniallys.created_at)
        and geniallys.created_at is not null
    {{ dbt_utils.group_by(n=4) }}
),

rolling_status as (
    select
        user_id,
        first_usage,
        date_day,
        day_period,
        was_active,
        status,
        countif(was_active) over (
            partition by user_id
            order by day_period
            rows between {{ week_days_minus }} preceding
                and current row
        ) as days_active_7d,
        countif(was_active) over (
            partition by user_id
            order by day_period
            rows between {{ month_days_minus }} preceding
                and current row
        ) as days_active_28d,
        case
            when day_period < {{ week_days }} then 'N'
            when countif(was_active) over (
                partition by user_id
                order by day_period
                rows between {{ week_days_minus }} preceding
                    and current row
                ) = 0
                then 'C'
            else 'A'
        end as status_7d,
        case
            when day_period < {{ month_days }} then 'N'
            when countif(was_active) over (
                partition by user_id
                order by day_period
                rows between {{ month_days_minus }} preceding
                    and current row
                ) = 0
                then 'C'
            else 'A'
        end as status_28d
   from user_day_traffic
),

users as (
    select * from {{ ref('users') }}
),

final as (
    select
        {{ dbt_utils.surrogate_key(['rolling_status.user_id', 'rolling_status.date_day']) }} as id,
        rolling_status.user_id,
        rolling_status.first_usage,
        rolling_status.date_day,
        rolling_status.day_period,
        rolling_status.was_active,
        rolling_status.status,
        rolling_status.status_7d,
        rolling_status.days_active_7d,
        rolling_status.status_28d,
        rolling_status.days_active_28d,
        lag(rolling_status.status_7d, {{ week_days }}) over (
            partition by rolling_status.user_id
            order by day_period asc
        ) as previous_status_7d,
        lag(rolling_status.status_28d, {{ month_days }}) over (
            partition by rolling_status.user_id
            order by day_period asc
        ) as previous_status_28d

    from rolling_status
)

select * from final
