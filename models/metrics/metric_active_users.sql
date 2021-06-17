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
        count(user_id) as n_active_users,
        sum(n_total_creations) as n_total_creations,
        sum(n_active_creations) as n_active_creations


    from active_users
    group by 1, 2, 3, 4, 5
    order by register_at asc
)

select * from final