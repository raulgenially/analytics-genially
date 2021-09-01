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
        count(user_id) as n_signups,
        countif({{ define_creator() }}) as n_creators,
        countif({{ define_publisher() }}) as n_publishers,
        countif({{ define_active_user(active_period=90) }}) as n_active_users_90,
        countif({{ define_active_user(active_period=60) }}) as n_active_users_60,
        countif({{ define_active_user(active_period=30) }}) as n_active_users_30,
        countif({{ define_promoter() }}) as n_promoters,

    from users
    where date(registered_at) >= date(2019, 1, 1)
    group by 1, 2, 3, 4, 5, 6
    order by registered_at asc
)

select * from final
