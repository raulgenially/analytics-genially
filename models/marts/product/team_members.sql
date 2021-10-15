with members as (
    select * from {{ ref('stg_team_members') }}
),

spaces as (
    select * from {{ ref('stg_team_spaces') }}
),

team_space_collaborators as (
    select * from {{ ref('src_genially_team_space_collaborators') }} 
),

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

final as (
    select
        members.team_member_id,

        members.email,
        members.member_role_name as role,
        members.team_name,

        members_owners.is_owner_of_some_space,
        if(members_have_spaces is not null, true, false) as has_spaces_other_than_common,

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
)

select * from final
