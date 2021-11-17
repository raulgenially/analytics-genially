with collaboratives as (
    select * from {{ ref('src_genially_collaboratives') }}
),

geniallys as (
    select * from {{ ref('stg_geniallys') }}
),

users as (
    select * from {{ ref('stg_users') }}
),

team_members as (
    select * from {{ ref('stg_team_members') }}
),

final as (
    select
        collaboratives.collaborative_id,

        collaboratives.user_email as collaborator_email,
        collaboratives.collaboration_type,
        geniallys.name,
        owners.plan as owner_plan,
        owners.sector as owner_sector,
        owners.role as owner_role,
        owners.country as owner_country,

        geniallys.is_active,
        geniallys.is_published,
        geniallys.is_in_social_profile,
        geniallys.is_visualized_last_90_days,
        geniallys.is_visualized_last_60_days,
        geniallys.is_visualized_last_30_days,
        owners.is_social_profile_active as is_owner_social_profile_active,

        collaboratives.genially_id,
        if(collaboration_type = 1, users.user_id, team_members.user_id) as user_id,
        collaboratives.user_owner_id,
        collaboratives.team_id as collaborative_team_id,
        geniallys.team_id as genially_team_id,
        geniallys.space_id as genially_space_id,

        collaboratives.created_at

    from collaboratives
    inner join geniallys
        on collaboratives.genially_id = geniallys.genially_id
    inner join users as owners
        on collaboratives.user_owner_id = owners.user_id
    left join users
        on collaboratives.user_id = users.user_id
    left join team_members -- Collaboratives user id is linked to team members when collaboration type is 4 (see https://github.com/Genially/scrum-genially/issues/7287)
        on collaboratives.user_id = team_members.team_member_id
    where collaboratives.user_owner_id = geniallys.user_id       
)

select * from final
