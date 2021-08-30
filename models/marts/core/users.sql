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
        countif(is_published = true) as n_published_creations,
        countif(is_deleted = false and is_published = true) as n_active_published_creations,
        countif(is_in_social_profile = true) as n_creations_in_social_profile,
        countif(is_deleted = false and is_in_social_profile = true) as n_active_creations_in_social_profile,
        max(is_visualized_last_90_days) as has_creation_visualized_last_90_days,
        max(is_visualized_last_60_days) as has_creation_visualized_last_60_days,
        max(is_visualized_last_30_days) as has_creation_visualized_last_30_days

    from geniallys
    group by 1
),

users_collaboratives_union as (
    select
        user_owner_id as user_id,
        max(is_published) as is_collaborator_of_published_creation,
        max(is_in_social_profile and is_owner_social_profile_active) as is_collaborator_of_creation_in_social_profile,
        max(is_visualized_last_90_days) as is_collaborator_of_creation_visualized_last_90_days, 
        max(is_visualized_last_60_days) as is_collaborator_of_creation_visualized_last_60_days, 
        max(is_visualized_last_30_days) as is_collaborator_of_creation_visualized_last_30_days, 

    from collaboratives
    group by 1

    union all

    select
        user_id,
        max(is_published) as is_collaborator_of_published_creation,
        max(is_in_social_profile and is_owner_social_profile_active) as is_collaborator_of_creation_in_social_profile,
        max(is_visualized_last_90_days) as is_collaborator_of_creation_visualized_last_90_days, 
        max(is_visualized_last_60_days) as is_collaborator_of_creation_visualized_last_60_days, 
        max(is_visualized_last_30_days) as is_collaborator_of_creation_visualized_last_30_days, 

    from collaboratives
    group by 1
),

users_collaboratives as (
    select
        user_id,
        max(is_collaborator_of_published_creation) as is_collaborator_of_published_creation,
        max(is_collaborator_of_creation_in_social_profile) as is_collaborator_of_creation_in_social_profile,
        max(is_collaborator_of_creation_visualized_last_90_days) as is_collaborator_of_creation_visualized_last_90_days,
        max(is_collaborator_of_creation_visualized_last_60_days) as is_collaborator_of_creation_visualized_last_60_days,
        max(is_collaborator_of_creation_visualized_last_30_days) as is_collaborator_of_creation_visualized_last_30_days,
    
    from users_collaboratives_union
    group by 1
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
        coalesce(users_creations.n_active_published_creations, 0) as n_active_published_creations,
        coalesce(users_creations.n_creations_in_social_profile, 0) as n_creations_in_social_profile,
        coalesce(users_creations.n_active_creations_in_social_profile, 0) as n_active_creations_in_social_profile,

        users.is_validated,
        users.is_social_profile_active,
        ifnull(has_creation_visualized_last_90_days, false) as has_creation_visualized_last_90_days,
        ifnull(has_creation_visualized_last_60_days, false) as has_creation_visualized_last_60_days,
        ifnull(has_creation_visualized_last_30_days, false) as has_creation_visualized_last_30_days,
        if(users_collaboratives.user_id is not null, true, false) as is_collaborator,
        ifnull(is_collaborator_of_published_creation, false) as is_collaborator_of_published_creation,
        ifnull(is_collaborator_of_creation_in_social_profile, false) as is_collaborator_of_creation_in_social_profile,
        ifnull(is_collaborator_of_creation_visualized_last_90_days, false) as is_collaborator_of_creation_visualized_last_90_days,
        ifnull(is_collaborator_of_creation_visualized_last_60_days, false) as is_collaborator_of_creation_visualized_last_60_days,
        ifnull(is_collaborator_of_creation_visualized_last_30_days, false) as is_collaborator_of_creation_visualized_last_30_days,
        
        users.registered_at,
        users.last_access_at

    from users
    left join users_creations
        on users.user_id = users_creations.user_id
    left join users_collaboratives
        on users.user_id = users_collaboratives.user_id
)

select * from final
