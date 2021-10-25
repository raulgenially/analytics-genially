with spaces as (
    select * from {{ ref('src_genially_team_spaces') }}
),

teams as (
    select * from {{ ref('src_genially_teams') }}
),

members as (
    select * from {{ ref('stg_team_members') }}
),

final as (
    select
        spaces.team_space_id,

        spaces.name,
        teams.name as team_name,
        spaces.icon,

        spaces.is_common,

        spaces.team_id,
        spaces.owner_id,

        spaces.created_at,
        members.confirmed_at as owner_confirmed_at,
        teams.created_at as team_created_at

    from spaces
    inner join teams
        on spaces.team_id = teams.team_id
    left join members
        on spaces.owner_id = members.team_member_id
    where (spaces.is_common = false
            and members.team_member_id is not null)
        or (spaces.is_common = true)
)

select * from final
