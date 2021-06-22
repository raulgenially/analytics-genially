with active_users as (
    select * from {{ ref('active_users') }}
),

final as (
    select
        DATE(registered_at) as registered_at,
        plan,
        sector,
        role, 
        market,
        count(user_id) as n_active_users,
        sum(n_total_creations) as n_total_creations,
        sum(n_active_creations) as n_active_creations


    from active_users
    group by 1, 2, 3, 4, 5
    having registered_at >= DATE(2019, 1, 1)
    order by registered_at asc
)

select * from final