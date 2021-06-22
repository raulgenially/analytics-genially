with geniallys as (
    select * from {{ ref('src_genially_geniallys') }}
),

users as (
    select * from {{ ref('src_genially_users') }}
),

templates as (
    select * from {{ ref('src_genially_templates') }}
),

-- Note that there are geniallys from people who are not users any more.

final as (
    select
        --- Genially fields
        geniallys.genially_id,
        
        geniallys.genially_type,
        geniallys.subscription_plan as genially_plan,
        
        geniallys.is_published,
        geniallys.is_deleted,
        geniallys.is_private,
        geniallys.is_password_free,
        geniallys.is_in_social_profile,
        geniallys.is_reusable,
        geniallys.is_inspiration,
        
        geniallys.user_id as genially_user_id,
        geniallys.reused_from_id,
        geniallys.from_template_id,
        
        geniallys.modified_at,
        geniallys.created_at,
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

        -- Template fields
        templates.template_id,
        templates.template_type,
        templates.genially_id as template_genially_id,
        templates.genially_to_view_id as template_genially_to_view_id
        

    from geniallys
    left join users
        on geniallys.user_id = users.user_id
    left join templates
        on geniallys.from_template_id = templates.template_id
)

select * from final