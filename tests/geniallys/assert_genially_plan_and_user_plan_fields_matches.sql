-- Info as to plan in both geniallys and users should match.
-- Until we know how to fix this.
{{
  config(
    severity='warn' 
  )
}}

with geniallys as (
    select * from {{ ref('geniallys') }}
),

final as (
    select
        genially_id,
        user_id,
        genially_plan,
        user_plan,
        created_at,

    from geniallys
    where genially_plan != user_plan 
        and is_deleted = false
)

select * from final