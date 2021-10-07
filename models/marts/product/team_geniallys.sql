{{
  config(
    materialized='view'
  )
}}

with geniallys as (
    select * from {{ ref('geniallys') }}
),

final as (
    select * from geniallys
    where team_name is not null -- Only retains geniallys from existing teams
)

select * from final
