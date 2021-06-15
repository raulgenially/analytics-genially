with active_users as (
    select * from {{ ref('active_users') }}
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
    order by register_at asc
)

select * from final