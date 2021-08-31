with users as (
    select * from {{ ref('users') }}
),

final as (
    select 
        -- Dimensions
        plan,
        sector,
        role,
        market,

        -- Metrics
        count(user_id) as n_users,
        sum(n_total_creations) as n_total_creations,
        sum(n_active_creations) as n_active_creations,
        sum(n_active_published_creations) as n_active_published_creations,

    from users
    group by 1, 2, 3, 4
)

select * from final
