 -- The creation date of a genially should be greather than user's registration date.

with geniallys_users as (
    select * from {{ ref('stg_geniallys') }}
),

final as (
    select
        genially_id,
        user_id,
        created_at,
        user_registered_at

    from geniallys_users
    where created_at < user_registered_at
)

select * from final