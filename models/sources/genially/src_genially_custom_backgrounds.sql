with custom_background as (
    select * from {{ source('genially', 'custombackground') }}
),

final as(
    select
        _id as custom_background_id,

        urlbackground as custom_background_url,

        iduser as user_id,
        idteam as team_id

    from custom_background
    where __hevo__marked_deleted = false
)

 select * from final