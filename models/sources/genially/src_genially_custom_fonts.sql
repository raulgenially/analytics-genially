with custom_font as (
    select * from {{ source('genially', 'customfont') }}
),

final as(
    select
        _id as custom_font_id,

        urlfont as custom_font_url,
        name as font_name,
        format as font_format,
        displayname as font_display_name,
        childrenfonts as font_children_fonts,

        iduser as user_id,
        idteam as team_id,
        copiedfrom as font_copied_from_id,

    from custom_font
    where __hevo__marked_deleted = false
)

 select * from final
