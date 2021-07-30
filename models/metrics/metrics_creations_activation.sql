with geniallys as (
    select * from {{ ref('geniallys') }}
),

users as (
    select * from {{ ref('users') }}
),

final as (
    select
        -- Dimensions
        date(geniallys.created_at) as created_at,
        users.plan as plan,
        {{ create_subscription_field('users.plan') }} as subscription,
        users.sector as sector, 
        users.role as role,
        users.market as market,
        geniallys.origin as origin,
        geniallys.category as category,

        -- Metrics
        count(geniallys.genially_id) as n_total_creations_by_registered_users,
        countif({{ define_activated_user() }}) as n_total_creations_by_activated_users,
        countif(geniallys.is_deleted = false) as n_active_creations_by_registered_users,
        countif(geniallys.is_deleted = false and {{ define_activated_user() }}) as n_active_creations_by_activated_users,
        countif(geniallys.is_deleted = false and geniallys.is_published = true) as n_published_creations_by_registered_users,
        countif(geniallys.is_deleted = false and geniallys.is_published = true and {{ define_activated_user() }}) as n_published_creations_by_activated_users

    from geniallys
    inner join users on geniallys.user_id = users.user_id
    where date(created_at) >= date(2019, 1, 1) 
        and is_created_before_registration = false
    group by 1, 2, 3, 4, 5, 6, 7, 8
    order by created_at asc
)

select * from final
