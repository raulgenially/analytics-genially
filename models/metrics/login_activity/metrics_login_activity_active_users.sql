with activity as (
    select * from {{ ref('metrics_login_activity') }}
),

final as (
    select
        -- Dimensions
        date_day,
        {{ place_main_dimension_fields('activity') }},
        signup_device,
        signup_channel,

        -- Metrics
        countif(status = 'New') as n_signups,
        countif(status = 'Current') as n_returning_users,
        countif(status in ('New', 'Current')) as n_daily_active_users,
        countif(status_7d = 'New') as n_signups_7d,
        countif(status_7d = 'Current') as n_returning_users_7d,
        countif(status_7d in ('New', 'Current')) as n_weekly_active_users,
        countif(status_28d = 'New') as n_signups_28d,
        countif(status_28d = 'Current') as n_returning_users_28d,
        countif(status_28d in ('New', 'Current')) as n_monthly_active_users

    from activity
    {{ dbt_utils.group_by(n=11) }}
)

select * from final
