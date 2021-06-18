with users as (
    select * from {{ ref('src_genially_users') }}
),

geniallys_users as (
    select * from {{ ref('stg_geniallys_users_joined') }}
),

users_creations as (
    select
        user_id,
        count(user_id) as n_total_creations,
        countif(is_deleted = False) as n_active_creations,
        min(created_at) as first_creation_at,
        max(created_at) as last_creation_at

    from geniallys_users
    where user_id is not null
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
        users.is_validated,
        users_creations.first_creation_at,
        users_creations.last_creation_at,
        users.registered_at,
        users.last_access_at

    from users
    left join users_creations
        on users.user_id = users_creations.user_id
)

select * from final