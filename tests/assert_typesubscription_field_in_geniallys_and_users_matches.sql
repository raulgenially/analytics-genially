{{
    config(
        severity='warn'
    )
}}

with geniallys as (
    select * from {{ source('genially', 'geniallys') }}
),

users as (
    select * from {{ source('genially', 'users') }}
),

final as (
    select
        count(geniallys._id)

    from geniallys
    inner join users 
        on geniallys.iduser = users._id
    where geniallys.typesubscription != users.typesubscription
)

select * from final