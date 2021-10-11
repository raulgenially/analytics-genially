with spaces as (
    select * from {{ ref('src_genially_team_spaces') }}
),

teams as (
    select * from {{ ref('src_genially_teams') }}
),

geniallys as (
    select * from {{ ref('team_geniallys') }}
),

geniallys_spaces as (
    select
        space_id,
        countif({{ define_active_creation('geniallys') }}) as n_active_creations

    from geniallys
    group by 1
), 

final as (
    select
        spaces.team_space_id,

        spaces.name,
        teams.name as team_name,
        coalesce(geniallys_spaces.n_active_creations, 0) as n_active_creations,

        spaces.is_common,

        spaces.team_id,
        spaces.owner_id,

        spaces.created_at

    from spaces
    left join teams
        on spaces.team_id = teams.team_id
    left join geniallys_spaces
        on spaces.team_space_id = geniallys_spaces.space_id
)

select * from final
