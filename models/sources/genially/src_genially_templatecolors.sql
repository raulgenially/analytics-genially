with templatecolors as (
    select * from {{ source('genially', 'templatecolors') }}
),

final as (
    select
        _id as templatecolor_id,

        color as color,

        idgenially as genially_id,
        idgeniallytoview as genially_to_view_id,
        idtemplate as template_id

    from templatecolors
    where __hevo__marked_deleted = false
)

select * from final
