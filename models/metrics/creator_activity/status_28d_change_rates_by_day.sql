with activity as (
    select * from {{ ref('creator_activity_by_day') }}
),

denominator as (
    select
        date_day,
        previous_status_28d,
        count(distinct user_id) as users

    from activity
    group by 1, 2
),

numerator as (
    select
        date_day,
        previous_status_28d,
        status_28d,
        count(distinct user_id) as users

    from activity
    where ((previous_status_28d = 'A' and status_28d = 'A')
         or (previous_status_28d = 'N' and status_28d = 'A')
         or (previous_status_28d = 'C' and status_28d = 'A'))
    {{ dbt_utils.group_by(n=3) }}
),

final as (
    select
        denominator.date_day,
        numerator.status_28d,
        numerator.previous_status_28d,
        numerator.users as active_users,
        denominator.users as all_users,
        numerator.users / (denominator.users * 1.0) as change_rate

    from denominator
    left join numerator
        on denominator.date_day = numerator.date_day
        and denominator.previous_status_28d = numerator.previous_status_28d

    -- Not sure about this one.
    where numerator.previous_status_28d is not null
        and numerator.status_28d is not null
)

select * from final
