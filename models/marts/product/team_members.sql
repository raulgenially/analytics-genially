with members as (
    select * from {{ ref('stg_team_members') }}
),

spaces as (
    select * from {{ ref('stg_team_spaces') }}
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

final as (
    select
        members.team_member_id,

        members.email,
        members.member_role_name as role,

        members_owners.is_owner_of_some_space,

        members.user_id,
        members.team_id,

        members.confirmed_at,
        members.deleted_at
    
    from members
    left join members_owners
        on members.team_member_id = members_owners.team_member_id
)

select * from final
