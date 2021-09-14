with teammember as (
    select * from {{ source('genially', 'teammember') }}
),

final as (
    select
        _id as team_member_id,

        email,
        role,

        iduser as user_id,
        idteam as team_id,

        confirmedat as confirmed_at,
        deletedat as deleted_at

    from teammember
    where __hevo__marked_deleted = false
)

select * from final
