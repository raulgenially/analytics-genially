-- Collaboratives of type 1 and teams-related fields are valid
with collaboratives as (
    select * from {{ ref('stg_collaboratives') }}
),

final as (
    select 
        *

    from collaboratives
    where collaboration_type = 1
        and (collaborative_team_id is not null
            or genially_team_id is not null)
)

select * from final
