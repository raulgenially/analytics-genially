with users as (
    select * from {{ ref('users') }}
),

final as (
    select 
        -- Dimensions
        date(registered_at) as registered_at,
        plan,
        if(plan = 'Free', 'Free', 'Premium') as subscription,
        sector,
        role,
        market,
        case
            when n_published_creations >= 5
                then '5+ creations'
            else concat(n_published_creations, ' creations')
        end as published_creations,

        -- Metrics
        count(user_id) as n_users,
        sum(n_total_creations) as n_total_creations,
        sum(n_active_creations) as n_active_creations,
        sum(n_published_creations) as n_published_creations,

    from users
    where date(registered_at) >= date(2019, 1, 1)
    group by 1, 2, 3, 4, 5, 6, 7
    order by registered_at asc
)

select * from final
