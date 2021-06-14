with active_users as (
    select * 
    from {{ ref('dim_users') }}
    where DATE_DIFF(CURRENT_DATE(), DATE(last_access_at), DAY) <= 90
),

final as (
    select
        DATE(register_at) as register_at,
        plan,
        sector,
        role, 
        market,
        count(user_id) as active_users

    from active_users
    group by 1, 2, 3, 4, 5
    order by register_at desc
)

select * from final