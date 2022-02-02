with activity as (
    select * from {{ ref('metrics_login_activity') }}
),

daily_change_rates as (
    {{ create_metrics_activity_status_change_rates_model('activity', 'status') }}
),

weekly_change_rates as (
    {{ create_metrics_activity_status_change_rates_model('activity', 'status_7d') }}
),

monthly_change_rates as (
    {{ create_metrics_activity_status_change_rates_model('activity', 'status_28d') }}
),

final as (
    select 
        'Daily' as time_period,
        *

    from daily_change_rates
    where transition_type in ('Activation', 'Retention', 'Resurrection')

    union all

    select 
        'Weekly' as time_period,
        *

    from weekly_change_rates
    where transition_type in ('Activation', 'Retention', 'Resurrection')

    union all

    select 
        'Monthly' as time_period,
        *

    from monthly_change_rates
    where transition_type in ('Activation', 'Retention', 'Resurrection')
)

select * from final
