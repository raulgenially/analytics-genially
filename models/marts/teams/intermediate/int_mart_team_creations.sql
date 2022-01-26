{{
    config(
        materialized='view'
    )
}}

with geniallys as (
    select * from {{ ref('stg_geniallys') }}
    where space_id is not null
        and is_active = true
),

final as (
    select
        user_id,
        space_id,
        team_id,
        count(genially_id) as n_active_creations

    from geniallys
    {{ dbt_utils.group_by(3) }}
)

select * from final
