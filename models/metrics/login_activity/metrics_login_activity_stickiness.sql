with activity as (
    select * from {{ ref('metrics_login_activity') }}
),

final as (
    select
        -- Dimensions
        date_day,
        {{ place_main_dimension_fields('activity') }},
        device,
        acquisition_channel,

        -- Metrics
        countif(is_active) as n_daily_active_users,
        countif(status_7d in ('New', 'Current')) as n_weekly_active_users,
        countif(status_28d in ('New', 'Current')) as n_monthly_active_users

    from activity
    {{ dbt_utils.group_by(n=10) }}
)

select * from final
