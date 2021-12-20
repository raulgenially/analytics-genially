with geniallys as (
    select * from {{ ref('stg_geniallys') }}
),

teams as (
    select * from {{ ref('src_genially_teams') }}
),

final as (
    select geniallys.genially_id

    from geniallys
    inner join teams
        on geniallys.team_id = teams.team_id
    where teams.is_disabled = true and geniallys.is_disabled = false
)
select * from final
