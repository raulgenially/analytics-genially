with spaces as (
    select * from {{ ref('src_genially_team_spaces') }}
),

teams as (
    select * from {{ ref('src_genially_teams') }}
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
        teams.created_at as team_created_at

    from spaces
    inner join teams
        on spaces.team_id = teams.team_id
)

select * from final
