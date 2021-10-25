with members as (
    select * from {{ ref('stg_team_members') }}
),

spaces as (
    select * from {{ ref('stg_team_spaces') }}
),

team_space_collaborators as (
    select * from {{ ref('src_genially_team_space_collaborators') }} 
),

geniallys as (
    select * from {{ ref('geniallys') }}
),

-- Extract members that are owners of some space
members_owners as (
    select
        members.team_member_id,
        max(
            if(members.team_member_id = spaces.owner_id, true, false)
        ) as is_owner_of_some_space

    from members
    left join spaces
        on members.team_id = spaces.team_id
    group by 1
),

-- These are members that have spaces in their Team workspace other than the common space 
members_have_spaces as (
    select
        distinct members.team_member_id 

    from members
    inner join team_space_collaborators
        on members.team_member_id = team_space_collaborators.collaborator_id
),

-- Compute geniallys created by a team member
members_geniallys as (
    select
        members.team_member_id,
        countif(geniallys.is_active = true) as n_active_creations

    from members
    inner join geniallys
        on members.user_id = geniallys.user_id
            and members.team_id = geniallys.team_id
    group by 1
),

-- Members that still make use of the personal workspace
members_geniallys_personal_ws as (
    select
        members.team_member_id,
        max(
            if(geniallys.is_active = true and geniallys.created_at >= members.team_created_at, true, false)
        ) as has_created_in_personal_ws

    from members
    left join geniallys
        on members.user_id = geniallys.user_id
        and geniallys.team_id is null -- Only retains those geniallys in personal ws
    group by 1
),

final as (
    select
        members.team_member_id,

        members.email,
        members.member_role_name as role,
        members.team_name,
        coalesce(members_geniallys.n_active_creations, 0) as n_active_creations,

        members_owners.is_owner_of_some_space,
        if(members_have_spaces.team_member_id is not null, true, false) as has_spaces_other_than_common,
        members_geniallys_personal_ws.has_created_in_personal_ws,

        members.user_id,
        members.team_id,

        members.confirmed_at,
        members.deleted_at,
        members.team_created_at
    
    from members
    left join members_owners
        on members.team_member_id = members_owners.team_member_id
    left join members_have_spaces
        on members.team_member_id = members_have_spaces.team_member_id
    left join members_geniallys
        on members.team_member_id = members_geniallys.team_member_id
    left join members_geniallys_personal_ws
        on members.team_member_id = members_geniallys_personal_ws.team_member_id
)

select * from final
