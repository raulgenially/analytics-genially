with team_spaces as (
    select * from {{ ref('src_genially_team_spaces') }}
),

teams as (
    select * from {{ ref('src_genially_teams') }}
),

geniallys as (
    select * from {{ ref('team_geniallys') }}
),

geniallys_in_spaces as (
    select
        space_id,
        countif({{ define_active_creation('geniallys') }}) as n_active_creations

    from geniallys
    group by 1
), 

final as (
    select
        team_spaces.team_space_id,

        team_spaces.name,
        teams.name as team_name,
        coalesce(geniallys_in_spaces.n_active_creations, 0) as n_active_creations,

        team_spaces.is_common,

        team_spaces.team_id,

        team_spaces.created_at

    from team_spaces
    left join teams
        on team_spaces.team_id = teams.team_id
    left join geniallys_in_spaces
        on team_spaces.team_space_id = geniallys_in_spaces.space_id
)

select * from final
