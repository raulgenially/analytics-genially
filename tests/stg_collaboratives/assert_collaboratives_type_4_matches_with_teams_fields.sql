-- Collaboratives of type 4 and teams-related fields are valid
with collaboratives as (
    select * from {{ ref('stg_collaboratives') }}
),

final as (
    select 
        *

    from collaboratives
    where collaboration_type = 4
        and (team_id is null
            or genially_team_id is null
            or team_id != genially_team_id)
)

select * from final
