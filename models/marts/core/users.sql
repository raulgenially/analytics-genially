with users as (
    select * from {{ ref('stg_users') }}
),

geniallys as (
    select * from {{ ref('stg_geniallys') }}
),

collaboratives as (
    select * from {{ ref('stg_collaboratives') }}
),

users_creations as (
    select
        user_id,
        count(genially_id) as n_total_creations,
        countif(is_deleted = false) as n_active_creations,
        countif(is_deleted = false and is_published = true) as n_published_creations,

    from geniallys
    group by 1
),

publisher_collaboratives as (
    select
        user_id,
        user_owner_id

    from collaboratives
    where is_published = true
),

final as (
    select
        users.user_id,

        users.plan,
        users.sector,
        users.role,
        users.market,
        users.email as email,
        users.social_profile_name,
        coalesce(users_creations.n_total_creations, 0) as n_total_creations,
        coalesce(users_creations.n_active_creations, 0) as n_active_creations,
        coalesce(users_creations.n_published_creations, 0) as n_published_creations,

        users.is_validated,
        users.is_social_profile_active,
        if(users.user_id in (select user_id from collaboratives) 
            or users.user_id in (select user_owner_id from collaboratives), true, false) as is_collaborator,
        if(users.user_id in (select user_id from publisher_collaboratives) 
            or users.user_id in (select user_owner_id from publisher_collaboratives), true, false) as is_collaborator_of_published_creation,
        users.registered_at,
        users.last_access_at

    from users
    left join users_creations
        on users.user_id = users_creations.user_id
)

select * from final
