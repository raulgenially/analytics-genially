{{
  config(
    materialized='view'
  )
}}

with geniallys as (
    select * from {{ ref('geniallys') }}
),

spaces as (
  select * from {{ ref('src_genially_team_spaces') }}
),

final as (
    select
        geniallys.*
    from geniallys
    inner join spaces
        on geniallys.space_id = spaces.team_space_id -- Only retains geniallys from existing spaces
)

select * from final
