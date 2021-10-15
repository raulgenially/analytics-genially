with activity as (
    select * from {{ ref('creator_activity_by_day') }}

),

final as (
    select
        date_day,
        case
            when status_28d='A' then 'Active Returning Creators'
            when status_28d='N' then 'New Creators'
            else status_28d
        end as status_28d,
        count(distinct user_id) as users

    from activity
    where status_28d in ('N', 'A')
    group by 1, 2

    union all

    select
        date_day as date,
        'Total Active Creators' as status_28d,
        count(distinct user_id) as users

    from activity
    where status_28d in ('N', 'A')
    group by 1, 2
)

select * from final
