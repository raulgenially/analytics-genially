-- Info as to plan in both geniallys and users should match.
{{ config(severity = 'warn') }} -- Until we know how to fix this

with geniallys as (
    select * from {{ ref('stg_geniallys') }}
),

final as (
    select
        genially_id,
        user_id,
        genially_plan,
        user_plan,
        created_at,
        user_registered_at

    from geniallys
    where genially_plan != user_plan 
        and is_deleted = False
    order by user_registered_at desc
)

select * from final