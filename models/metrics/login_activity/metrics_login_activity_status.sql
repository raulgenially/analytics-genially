with activity as (
    select * from {{ ref('metrics_login_activity') }}

),

final as (
    select
        date_day,
        case
            when status = 'Current'
                then 'Current Users'
            when status = 'New'
                then 'Signups'
            else status
        end as status,
        count(user_id) as n_users

    from activity
    where status in ('New', 'Current')
    group by 1, 2

    union all

    select
        date_day,
        'Total Active Users' as status,
        count(user_id) as n_users

    from activity
    where status in ('New', 'Current')
    group by 1, 2
)

select * from final
