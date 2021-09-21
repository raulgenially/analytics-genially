with geniallys as (
    select * from {{ ref('src_genially_geniallys') }}
),

templates as (
    select * from {{ ref('src_genially_templates') }}
),

templatecolors as (
    select * from {{ ref('stg_templatecolors') }}
),

inspiration as (
    select * from {{ ref('src_genially_inspiration') }}
),

total_templates as(
    select
        *

    from templates
    union all 
    select
        *

    from templatecolors
),

genially_templates as (
    select
        geniallys.genially_id,

    from geniallys
    inner join templates
        on geniallys.genially_id = templates.genially_id
),

genially_templates_view as (
    select
        geniallys.genially_id,

    from geniallys
    inner join templates
        on geniallys.genially_id = templates.genially_to_view_id
),

final as (
    select
        --- Genially fields
        geniallys.genially_id,

        geniallys.subscription_plan as plan,
        geniallys.name,
        case
            when geniallys.reused_from_id is not null
                then
                    case
                        when inspiration_id is null
                            then 'Reusable'
                        else 'Inspiration Reusable'
                    end
            when geniallys.from_template_id is not null
                then 'Template'
            when geniallys.from_team_template_id is not null
                then 'Team Template'
            when geniallys.genially_type = 17 or geniallys.genially_type = 27
                then 'From Scratch'
            when geniallys.genially_type = 18
                then 'PPTX Import'
            else
                'Other'
        end as source,
        {{ map_genially_category('total_templates.template_type', 'geniallys.genially_type') }} as category,
        total_templates.template_type,
        total_templates.name as template_name,

        geniallys.is_published,
        geniallys.is_in_recyclebin,
        geniallys.is_logic_deleted,
        geniallys.is_deleted,
        geniallys.is_private,
        geniallys.is_password_free,
        geniallys.is_in_social_profile,
        geniallys.is_reusable,
        geniallys.is_inspiration,
        {{ create_visualization_period_field_for_creation('geniallys.last_view_at', 90) }} as is_visualized_last_90_days,
        {{ create_visualization_period_field_for_creation('geniallys.last_view_at', 60) }} as is_visualized_last_60_days,
        {{ create_visualization_period_field_for_creation('geniallys.last_view_at', 30) }} as is_visualized_last_30_days,

        geniallys.user_id,
        geniallys.reused_from_id,
        geniallys.from_template_id,

        geniallys.created_at,
        geniallys.modified_at,
        geniallys.published_at,
        geniallys.last_view_at,
        geniallys.deleted_at,

    from geniallys
    left join total_templates
        on geniallys.from_template_id = total_templates.template_id
     -- Remove geniallys that are templates
    left join genially_templates
        on geniallys.genially_id = genially_templates.genially_id
    left join genially_templates_view
        on geniallys.genially_id = genially_templates_view.genially_id
    left join inspiration
        on geniallys.reused_from_id = inspiration.genially_id    
    where genially_templates.genially_id is null
        and genially_templates_view.genially_id is null
)

select * from final
