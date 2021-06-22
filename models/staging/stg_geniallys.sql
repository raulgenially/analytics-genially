with geniallys as (
    select * from {{ ref('src_genially_geniallys') }}
),

users as (
    select * from {{ ref('src_genially_users') }}
),

templates as (
    select * from {{ ref('src_genially_templates') }}
),

geniallys_with_decoded_type as (
    select
        geniallys.genially_id,
        case
            when template_type is not null
                then template_type
            when geniallys.genially_type = 17
                then 'blank-creation'
            when geniallys.genially_type = 27
                then 'interactive-image'
            when geniallys.genially_type = 18
                then 'ppt-importer'
            else 'other'
        end as genially_type

    from geniallys
    -- Remove geniallys that are templates
    left join templates
        on geniallys.from_template_id = templates.template_id
    where geniallys.genially_id not in (select genially_id from templates) 
        and geniallys.genially_id not in (select genially_to_view_id from templates) 

),

final as (
    select
        --- Genially fields
        geniallys.genially_id,
        
        geniallys_with_decoded_type.genially_type,
        geniallys.subscription_plan as genially_plan,
        
        geniallys.is_published,
        geniallys.is_deleted,
        geniallys.is_private,
        geniallys.is_password_free,
        geniallys.is_in_social_profile,
        geniallys.is_reusable,
        geniallys.is_inspiration,
        
        geniallys.user_id as genially_user_id,
        case
            when users.user_id is not null
                then True
            else False
        end as is_current_user,
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
      
    from geniallys
    inner join geniallys_with_decoded_type
        on geniallys.genially_id = geniallys_with_decoded_type.genially_id
    left join users
        on geniallys.user_id = users.user_id
)

select * from final