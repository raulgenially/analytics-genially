-- Team collaboration should not happen with a
-- genially that is not part of a team workspace
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
    where collaboratives.collaboration_type = 4
        and not (collaboratives.team_id is not null
                    and geniallys.team_id is not null
                    and collaboratives.team_id = geniallys.team_id)
)

select * from final

