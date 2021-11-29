with activity as (
    select * from {{ ref('metrics_login_activity') }}

),

final as (
    select
        date_day,
        case
            when status_28d = 'Active'
                then 'Active Returning Users'
            when status_28d = 'New'
                then 'Active New Users'
            else status_28d
        end as status_28d,
        count(user_id) as n_users

    from activity
    where status_28d in ('New', 'Active')
    group by 1, 2

    union all

    select
        date_day,
        'Total Active Users' as status_28d,
        count(user_id) as n_users

    from activity
    where status_28d in ('New', 'Active')
    group by 1, 2
)

select * from final
