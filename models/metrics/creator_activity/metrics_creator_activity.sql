{% set week_days = 7 %}
{% set week_days_minus = week_days - 1 %}

{% set month_days = 28 %}
{% set month_days_minus = month_days - 1 %}

{% set min_date %}
    date('2019-01-01')
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
        start_date=min_date,
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
        {{ dbt_utils.surrogate_key(['user_id', 'date_day']) }} as id,
        user_id,
        first_usage_at,
        date_day,
        n_days_since_first_usage,
        is_active,
        status,
        n_days_active_7d,
        status_7d,
        n_days_active_28d,
        status_28d,
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
