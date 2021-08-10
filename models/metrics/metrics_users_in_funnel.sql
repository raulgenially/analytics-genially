with users as (
    select * from {{ ref('users') }}
),

final as (
    select 
        -- Dimensions
        date(registered_at) as registered_at,
        plan,
        {{ create_subscription_field('plan') }} as subscription,
        sector,
        role,
        market,

        -- Metrics
        count(user_id) as n_registered_users,
        countif({{ define_activated_user() }}) as n_activated_users,
        countif({{ define_active_user() }}) as n_active_users,
        countif({{ define_promoter_user() }}) as n_promoter_users,

    from users
    where date(registered_at) >= date(2019, 1, 1)
    group by 1, 2, 3, 4, 5, 6
    order by registered_at asc
)

select * from final