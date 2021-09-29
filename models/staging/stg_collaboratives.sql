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

final as (
    select
        collaboratives.collaborative_id,

        collaboratives.collaboration_type,
        geniallys.is_published,
        geniallys.is_deleted,
        geniallys.is_in_social_profile,
        geniallys.is_visualized_last_90_days,
        geniallys.is_visualized_last_60_days,
        geniallys.is_visualized_last_30_days,
        owners.is_social_profile_active as is_owner_social_profile_active,

        collaboratives.genially_id,
        case
            when collaboration_type = 1
                then users.user_id
            else team_members_users.user_id
        end as user_id,
        owners.user_id as user_owner_id

    from collaboratives
    inner join geniallys
        on collaboratives.genially_id = geniallys.genially_id
    left join users as owners
        on collaboratives.user_owner_id = owners.user_id
    left join users
        on collaboratives.user_id = users.user_id
    left join team_members_users
        on collaboratives.user_id = team_members_users.team_member_id
    where collaboratives.user_owner_id = geniallys.user_id       
)

select * from final
