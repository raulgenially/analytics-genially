 -- The creation date of a genially should be greather than user's registration date.

with geniallys as (
    select * from {{ ref('stg_geniallys') }}
),

final as (
    select
        genially_id,
        user_id,
        created_at,
        user_registered_at

    from geniallys
    where created_at < user_registered_at and is_deleted = False
    order by user_registered_at desc
)

select * from final