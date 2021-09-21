with templatecolors as (
    select * from {{ ref('src_genially_templatecolors') }}
),

templates as (
    select * from {{ ref('src_genially_templates') }}
),

final as (
    select
        templatecolors.genially_id as template_id,

        templates.template_type,
        templates.subcategory,
        templates.name,
        templates.language,
        templates.tags,
        templates.template_order,
        templates.default_color,

        templates.is_hidden,
        templates.is_premium,
        templates.is_new,
        templates.has_slides_selectable,

        templatecolors.genially_id,
        templatecolors.genially_to_view_id,

    from templatecolors
    inner join templates
        on templatecolors.template_id = templates.template_id
)

select * from final
