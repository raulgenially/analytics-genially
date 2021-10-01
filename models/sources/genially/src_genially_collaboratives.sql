with collaboratives as (
    select
        _id as collaborative_id,

        email as user_email,
        ifnull(collaborationtype, 1) as collaboration_type,

        idgenially as genially_id,
        iduser as user_id,
        iduserowner as user_owner_id,
        idteam as team_id,

        createat as created_at

    from {{ source('genially', 'collaborative') }}
    where __hevo__marked_deleted = false
        -- I'm commenting for now because all collaboratives of type 4 (Teams context) have email set to Null
        --and user_email is not null
)

select * from collaboratives
