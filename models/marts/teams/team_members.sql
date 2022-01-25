with members as (
    select * from {{ ref('src_genially_team_members') }}
),

creations as (
    select * from {{ ref('int_mart_team_creations') }}
),

-- Compute geniallys created by a team member
member_creations as (
    select
        members.team_member_id,
        sum(creations.n_active_creations) as n_active_creations,

    from members
    inner join creations
        on members.user_id = creations.user_id
            and members.team_id = creations.team_id
    group by 1
),

final as (
    select
        members.team_member_id,

        members.email,
        members.member_role_name as role,
        coalesce(member_creations.n_active_creations, 0) as n_active_creations,

        members.is_part_of_the_team,

        members.user_id,
        members.team_id,

        members.confirmed_at,
        members.deleted_at,
        members.created_at,

    from members
    left join member_creations
        on members.team_member_id = member_creations.team_member_id
)

select * from final
