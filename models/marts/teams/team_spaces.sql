with spaces as (
    select * from {{ ref('src_genially_team_spaces') }}
),

creations as (
    select * from {{ ref('int_mart_team_creations') }}
),

space_creations as (
    select
        space_id,
        sum(n_active_creations) as n_active_creations

    from creations
    group by 1
),

final as (
    select
        spaces.team_space_id,

        spaces.name,
        coalesce(space_creations.n_active_creations, 0) as n_active_creations,

        spaces.is_common,

        spaces.team_id,
        spaces.owner_id,

        spaces.created_at,

    from spaces
    left join space_creations
        on spaces.team_space_id = space_creations.space_id
)

select * from final
