with custom_color as (
    select * from {{ source('genially', 'customcolor') }}
),

final as(
    select
        _id as custom_color_id,

        color as custom_color_code,

        iduser as user_id,
        idteam as team_id

    from custom_color
    where __hevo__marked_deleted = false
)

 select * from final limit 10