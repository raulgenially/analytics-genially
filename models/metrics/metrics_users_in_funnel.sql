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
        broad_sector,
        role,
        country,
        country_name,

        -- Metrics
        count(user_id) as n_signups,
        countif({{ define_creator() }}) as n_creators,
        countif({{ define_publisher() }}) as n_publishers,
        countif({{ define_recurrent_publisher() }}) as n_recurrent_publishers,
        countif({{ define_heavy_publisher() }}) as n_heavy_publishers

    from users
    where date(registered_at) >= date(2019, 1, 1) and date(registered_at) < current_date() 
    group by 1, 2, 3, 4, 5, 6, 7, 8
    order by registered_at asc
)

select * from final
