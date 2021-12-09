with spaces as (
    select * from {{ ref('stg_team_spaces') }}
),

geniallys as (
    select * from {{ ref('team_geniallys') }}
),

geniallys_spaces as (
    select
        space_id,
        countif(is_active = true) as n_active_creations

    from geniallys
    group by 1
), 

final as (
    select
        spaces.team_space_id,

        spaces.name,
        spaces.team_name,
        coalesce(geniallys_spaces.n_active_creations, 0) as n_active_creations,

        spaces.is_common,

        spaces.team_id,
        spaces.owner_id,

        spaces.created_at,
        spaces.team_created_at

    from spaces
    left join geniallys_spaces
        on spaces.team_space_id = geniallys_spaces.space_id
)

select * from final
