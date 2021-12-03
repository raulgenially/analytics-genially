with activity as (
    select * from {{ ref('metrics_login_activity') }}

),

final as (
    select
        -- Dimensions
        date_day,
        case
            when status_28d = 'Current'
                then 'Current Users'
            when status_28d = 'New'
                then 'Signups'
            else status_28d
        end as status_28d,
        {{ place_main_dimension_fields('activity') }}

        -- Metrics
        count(user_id) as n_users

    from activity
    where status_28d in ('New', 'Current')
    {{ dbt_utils.group_by(n=9) }}

    union all

    select
        -- Dimensions
        date_day,
        'Total Active Users' as status_28d,
        {{ place_main_dimension_fields('activity') }}

        -- Metrics
        count(user_id) as n_users

    from activity
    where status_28d in ('New', 'Current')
    {{ dbt_utils.group_by(n=9) }}
)

select * from final
