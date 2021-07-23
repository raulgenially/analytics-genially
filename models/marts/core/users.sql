with users as (
    select * from {{ ref('src_genially_users') }}
),

geniallys as (
    select * from {{ ref('stg_geniallys') }}
),

users_creations as (
    select
        user_id,
        count(genially_id) as n_total_creations,
        countif(is_deleted = false) as n_active_creations,
        countif(is_deleted = false and is_published = true) as n_published_creations

    from geniallys
    where is_from_current_user = true
    group by 1
),

final as (
    select
        users.user_id,

        users.subscription_plan as plan,
        users.sector,
        users.role,
        users.country as market,
        coalesce(users_creations.n_total_creations, 0) as n_total_creations,
        coalesce(users_creations.n_active_creations, 0) as n_active_creations,
        coalesce(users_creations.n_published_creations, 0) as n_published_creations,

        users.is_validated,

        users.registered_at,
        users.last_access_at

    from users
    left join users_creations
        on users.user_id = users_creations.user_id
)

select * from final