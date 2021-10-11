{{
  config(
    materialized='view'
  )
}}

with geniallys as (
    select * from {{ ref('geniallys') }}
),

teams as (
  select * from {{ ref('src_genially_teams') }}
),

final as (
    select
        geniallys.*
      
    from geniallys
    inner join teams
        on geniallys.team_id = teams.team_id -- Only retains geniallys from existing teams
    where space_id is not null
)

select * from final
