with geniallys as (
    select * from {{ ref('src_genially_geniallys') }}
),

users as (
    select * from {{ ref('src_genially_users') }}
),

templates as (
    select * from {{ ref('src_genially_templates') }}
),

collaboratives as (
    select * from {{ ref('src_genially_collaboratives') }}
),

final as (
    select
        --- Genially fields
        geniallys.genially_id,
        
        geniallys.subscription_plan as genially_plan,
        case
            when reused_from_id is not null
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
        if(geniallys.genially_id in 
            (select genially_id from collaboratives), true, false) as is_collaborative,
        
        geniallys.user_id as genially_user_id,
        if(users.user_id is not null, true, false) as is_from_current_user,
        geniallys.reused_from_id,
        geniallys.from_template_id,
        templates.template_type,
        templates.name as template_name,
        
        geniallys.modified_at,
        geniallys.created_at,
        -- In some cases creation date < registration date
        case
            when geniallys.created_at is null or users.registered_at is null
                then null
            else geniallys.created_at < users.registered_at
        end as is_created_before_registration,
        geniallys.published_at,
        geniallys.last_view_at,
        geniallys.deleted_at,

        -- User fields
        users.user_id as user_id,
        users.subscription_plan as user_plan,
        users.sector as user_sector,
        users.role as user_role,
        users.country as user_market,
        users.is_validated as user_is_validated,  
        users.registered_at as user_registered_at,
        users.last_access_at as user_last_access_at,
      
    from geniallys
    left join templates
        on geniallys.from_template_id = templates.template_id
    left join users
        on geniallys.user_id = users.user_id
     -- Remove geniallys that are templates
    where geniallys.genially_id not in (select genially_id from templates) 
        and geniallys.genially_id not in (select genially_to_view_id from templates)
)

select * from final