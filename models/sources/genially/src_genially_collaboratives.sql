with collaboratives as (
    select
        _id as collaborative_id,

        email as user_email,

        idgenially as genially_id,
        iduser as user_id,
        iduserowner as user_owner_id,
        idteam as team_id,

        createat as created_at

    from {{ source('genially', 'collaborative') }}
    where __hevo__marked_deleted = false
        and email is not null
        and iduser != iduserowner
)

select * from collaboratives
