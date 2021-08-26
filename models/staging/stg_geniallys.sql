with geniallys as (
    select * from {{ ref('src_genially_geniallys') }}
),

templates as (
    select * from {{ ref('src_genially_templates') }}
),

final as (
    select
        --- Genially fields
        geniallys.genially_id,
        
        geniallys.subscription_plan as plan,
        case
            when geniallys.reused_from_id is not null
                then 'Reusable'
            when templates.template_type is not null
                then 'Template'
            when geniallys.genially_type = 17 or geniallys.genially_type = 27
                then 'From Scratch'
            when geniallys.genially_type = 18
                then 'PPTX Import'
            else
                'Other'
        end as origin,
        {{ map_genially_category('templates.template_type', 'geniallys.genially_type') }} as category, -- TODO fix mapping

        geniallys.is_published,
        geniallys.is_deleted,
        geniallys.is_private,
        geniallys.is_password_free,
        geniallys.is_in_social_profile,
        geniallys.is_reusable,
        geniallys.is_inspiration,
               
        geniallys.user_id,
        geniallys.reused_from_id,
        geniallys.from_template_id,
        templates.template_type,
        templates.name as template_name,
        
        geniallys.modified_at,
        geniallys.created_at,
        geniallys.published_at,
        geniallys.last_view_at,
        geniallys.deleted_at,
      
    from geniallys
    left join templates
        on geniallys.from_template_id = templates.template_id
     -- Remove geniallys that are templates
    where geniallys.genially_id not in (select genially_id from templates) 
        and geniallys.genially_id not in (select genially_to_view_id from templates)
)

select * from final
