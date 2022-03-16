 -- The creation date of a genially should be greather than user's registration date.
 -- TODO: https://github.com/Genially/scrum-genially/issues/9153
{{
    config(
        severity="warn",
    )
}}

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
        and users.registered_at > '2021-01-01'
    order by users.registered_at desc
)

select * from final
