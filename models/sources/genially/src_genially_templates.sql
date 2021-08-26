with templates as (
    select * from {{ source('genially', 'templatev9') }}
),

final as (
    select
        _id as template_id,

        templatetype as template_type,
        subcategory,
        name,
        language,
        tags,
        cast(templates.order as int64) as template_order,
        
        templates.new as is_new,
        selectslides as has_slides_selectable,
        
        idgenially as genially_id,
        idgeniallytoview as genially_to_view_id
   
    from templates
    where __hevo__marked_deleted = false
)

select * from final
