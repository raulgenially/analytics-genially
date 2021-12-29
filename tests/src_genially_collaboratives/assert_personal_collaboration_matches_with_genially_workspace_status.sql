-- Personal collaboration should not happen with a
-- genially that is part of a team workspace
-- TODO: https://github.com/Genially/scrum-genially/issues/8027
{{
    config(
        severity='warn'
    )
}}
with collaboratives as (
    select * from {{ ref('src_genially_collaboratives') }}
),

geniallys as (
    select * from {{ ref('src_genially_geniallys') }}
),

final as (
    select
        collaboratives.*

    from collaboratives
    left join geniallys
        on collaboratives.genially_id = geniallys.genially_id
    where collaboratives.collaboration_type = 1
        and not (collaboratives.team_id is null
                    and geniallys.team_id is null)
)

select * from final

