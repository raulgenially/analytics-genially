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
        templates.order as template_order,
        
        templates.new as is_new,
        
        idgenially as genially_id
   
    from templates
)

select * from final