with collaboratives as (
    select * from {{ ref('stg_collaboratives') }}
),

users as (
    select * from {{ ref('stg_users') }}
),

final as (
    select
        collaboratives.collaborative_id,
        
        collaboratives.collaborator_email,
        users.plan as collaborator_plan,
        users.sector as collaborator_sector,
        users.role as collaborator_role,
        users.country as collaborator_country,
        collaboratives.collaboration_type,
        collaboratives.name,
        collaboratives.owner_plan,
        collaboratives.owner_sector,
        collaboratives.owner_role,
        collaboratives.owner_country,

        collaboratives.is_active,
        collaboratives.is_published,
        collaboratives.is_in_social_profile,
        collaboratives.is_visualized_last_90_days,
        collaboratives.is_visualized_last_60_days,
        collaboratives.is_visualized_last_30_days,
        users.is_social_profile_active as is_collaborator_social_profile_active,
        collaboratives.is_owner_social_profile_active,

        collaboratives.genially_id,
        collaboratives.user_id,
        collaboratives.user_owner_id,
        collaboratives.collaborative_team_id as team_id,
        collaboratives.genially_space_id as space_id,

        collaboratives.created_at

    from collaboratives
    left join users
        on collaboratives.user_id = users.user_id
)

select * from final
