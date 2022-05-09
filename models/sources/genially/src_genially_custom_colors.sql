with custom_color as (
    select * from {{ source('genially', 'customcolor') }}
),

final as(
    select
        _id as color_id,

        color as color_code,

        iduser as user_id,
        idteam as team_id

    from custom_color
    where __hevo__marked_deleted = false
)

 select * from final
