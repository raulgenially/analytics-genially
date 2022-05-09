with custom_font as (
    select * from {{ source('genially', 'customfont') }}
),

final as(
    select
        _id as font_id,

        urlfont as font_url,
        name as font_name,
        format as font_format,
        displayname as display_name,
        childrenfonts as children_fonts,

        iduser as user_id,
        idteam as team_id,
        copiedfrom as copied_from_font_id,

    from custom_font
    where __hevo__marked_deleted = false
)

 select * from final
