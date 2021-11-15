with templatecolors as (
    select * from {{ ref('src_genially_templatecolors') }}
),

templates as (
    select * from {{ ref('src_genially_templates') }}
),

main_templates as (
    select
        template_id,
        template_type,
        subcategory,
        name,
        language,
        tags,
        template_order,
        default_color as color,

        is_hidden,
        is_premium,
        is_new,
        has_slides_selectable,

        genially_id,
        genially_to_view_id

    from templates
),

color_templates as (
    select
        templates.template_id,
        templates.template_type,
        templates.subcategory,
        templates.name,
        templates.language,
        templates.tags,
        templates.template_order,
        templatecolors.color,

        templates.is_hidden,
        templates.is_premium,
        templates.is_new,
        templates.has_slides_selectable,

        templatecolors.genially_id,
        templatecolors.genially_to_view_id

    from templates
    inner join templatecolors
        on templates.template_id = templatecolors.template_id
),

int_templates as (
    select * from main_templates
    union all
    select * from color_templates
),

final as (
    select
        {{ dbt_utils.surrogate_key([
            'template_id',
            'genially_id'
        ]) }} as id,

        *
    from int_templates
)

select * from final
