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
    select * from {{ ref('src_genially_team_members') }}
),

team_members_users as (
    select
        team_members.team_member_id,
        team_members.user_id

    from team_members
    inner join users
        on team_members.user_id = users.user_id
),

collaboratives_joined as (
    select
        collaboratives.collaborative_id,

        collaboratives.user_email,
        collaboratives.collaboration_type,
        geniallys.name,

        geniallys.is_published,
        geniallys.is_deleted,
        geniallys.is_in_social_profile,
        geniallys.is_visualized_last_90_days,
        geniallys.is_visualized_last_60_days,
        geniallys.is_visualized_last_30_days,
        owners.is_social_profile_active as is_owner_social_profile_active,

        collaboratives.genially_id,
        if(collaboration_type = 1, users.user_id, team_members_users.user_id) as user_id,
        collaboratives.user_owner_id,
        collaboratives.team_id,
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
    left join team_members_users
        on collaboratives.user_id = team_members_users.team_member_id
    where collaboratives.user_owner_id = geniallys.user_id       
),

final as (
    select 
        *

    from collaboratives_joined
    where user_owner_id != user_id -- This also removes null values of user_id
)

select * from final
