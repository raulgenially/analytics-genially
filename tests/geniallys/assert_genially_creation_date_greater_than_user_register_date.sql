 -- The creation date of a genially should be greather than user's registration date.

with geniallys as (
    select * from {{ ref('geniallys') }}
),

users as (
    select * from {{ ref('users') }}
),

final as (
    select
        geniallys.genially_id,
        geniallys.user_id,
        geniallys.created_at,
        users.registered_at

    from geniallys
    inner join users
        on geniallys.user_id = users.user_id
    where geniallys.created_at < users.registered_at 
        and geniallys.is_deleted = false
        and geniallys.is_created_before_registration = false
    order by users.registered_at desc
)

select * from final
