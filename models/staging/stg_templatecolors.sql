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
        /* We have the same genially_to_view_id with different names and language to show it depending user language.
        We can not know from what language variation a genially is created so doing this max() we avoid duplicates */
        max(templates.name) as name,
        max(templates.language) as language,
        max(templates.tags) as tags,
        templates.template_order,
        templates.default_color,

        templates.is_hidden,
        templates.is_premium,
        templates.is_new,
        templates.has_slides_selectable,

        templatecolors.genially_id as genially_id,
        templatecolors.genially_to_view_id as genially_to_view_id,

    from templatecolors
    inner join templates
        on templatecolors.template_id = templates.template_id
    group by 1,2,3,7,8,9,10,11,12,13,14
)

select * from final