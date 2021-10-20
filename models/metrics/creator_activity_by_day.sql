{% set partitions_to_replace = [
  'date_sub(current_date, interval 1 day)'
] %}

{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by={'field': 'date_day', 'data_type': 'date'},
    partitions = partitions_to_replace
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
    select
        user_id,
        min(date(created_at)) as first_usage_at

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
        user_usage.first_usage_at,
        date(dates.date_day) as date_day,
        date_diff(dates.date_day, user_usage.first_usage_at, day) as n_days_since_first_usage

    from user_usage
    cross join dates
    where user_usage.first_usage_at >= {{ min_date }} -- Only focus on creators from min_date on
        and dates.date_day >= user_usage.first_usage_at
),

user_day_traffic as (
    select
        user_day.user_id,
        user_day.first_usage_at,
        user_day.date_day,
        user_day.n_days_since_first_usage,
        max(geniallys.user_id is not null) as is_active,
        case
            when user_day.n_days_since_first_usage = 0 
                then 'New'
            when user_day.n_days_since_first_usage > 0 and max(geniallys.user_id is not null) = false 
                then 'Churned'
            when user_day.n_days_since_first_usage > 0 and max(geniallys.user_id is not null) = true 
                then 'Active'
        end as status

    from user_day
    left join geniallys
        on user_day.user_id = geniallys.user_id
            and user_day.date_day = date(geniallys.created_at)
            and geniallys.created_at is not null
    {{ dbt_utils.group_by(n=4) }}
),

user_traffic_rolling_status as (
    select
        user_id,
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
            else 'Active'
        end as status_7d,
        case
            when n_days_since_first_usage < {{ month_days }} 
                then 'New'
            when {{ compute_n_days_active(month_days_minus) }} = 0
                then 'Churned'
            else 'Active'
        end as status_28d
    
    from user_day_traffic
),

final as (
    select
        {{ dbt_utils.surrogate_key(['user_traffic_rolling_status.user_id', 'user_traffic_rolling_status.date_day']) }} as id,
        user_traffic_rolling_status.user_id,
        user_traffic_rolling_status.first_usage_at,
        user_traffic_rolling_status.date_day,
        user_traffic_rolling_status.n_days_since_first_usage,
        user_traffic_rolling_status.is_active,
        user_traffic_rolling_status.status,
        user_traffic_rolling_status.n_days_active_7d,
        user_traffic_rolling_status.status_7d,
        user_traffic_rolling_status.n_days_active_28d,
        user_traffic_rolling_status.status_28d,
        lag(user_traffic_rolling_status.status_7d, {{ week_days }}) over (
            partition by user_traffic_rolling_status.user_id
            order by n_days_since_first_usage asc
        ) as previous_status_7d,
        lag(user_traffic_rolling_status.status_28d, {{ month_days }}) over (
            partition by user_traffic_rolling_status.user_id
            order by n_days_since_first_usage asc
        ) as previous_status_28d

    from user_traffic_rolling_status
    
    {% if is_incremental() %}
    where user_traffic_rolling_status.date_day >= date_sub(current_date(), interval 1 day)
    {% endif %}
)

select * from final
