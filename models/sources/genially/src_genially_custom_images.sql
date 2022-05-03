with custom_image as (
    select * from {{ source('genially', 'customimage') }}
),

final as(
    select
        _id as custom_image_id,

        urlimage as custom_image_url,

        iduser as user_id,
        idteam as team_id

    from custom_image
    where __hevo__marked_deleted = false
)

 select * from final