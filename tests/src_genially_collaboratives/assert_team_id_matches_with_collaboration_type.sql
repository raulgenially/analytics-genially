-- A collaboration of type 4 is associated with a Team id
with collaboratives as (
    select * from {{ ref('src_genially_collaboratives') }}
),

final as (
    select
        *

    from collaboratives
    where (team_id is not null
            and collaboration_type = 1)
        or (team_id is null
            and collaboration_type = 4)
)

select * from final
