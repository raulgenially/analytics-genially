with geniallys as (
    select * from {{ ref('stg_geniallys') }}
),

users as (
    select * from {{ ref('stg_users') }}
    where is_deleted = false
),

final as (
    select
        geniallys.genially_id,

        geniallys.name,
        geniallys.source,
        geniallys.category,
        geniallys.template_type,
        geniallys.template_name,
        geniallys.template_language,
        geniallys.collaboration_type,
        users.plan as user_plan,
        users.sector as user_sector,
        users.broad_sector as user_broad_sector,
        users.role as user_role,
        users.broad_role as user_broad_role,
        users.country as user_country,
        users.country_name as user_country_name,

        geniallys.is_from_premium_template,
        geniallys.is_published,
        geniallys.is_active,
        geniallys.is_disabled,
        geniallys.is_private,
        geniallys.has_password,
        geniallys.is_in_social_profile,
        geniallys.is_reusable,
        geniallys.is_inspiration,
        geniallys.is_collaborative,
        geniallys.has_internal_analytics,

        geniallys.user_id,
        geniallys.reused_from_id,
        geniallys.team_id,
        geniallys.space_id,
        geniallys.template_to_view_id,
        geniallys.google_analytics_id,

        geniallys.created_at,
        geniallys.modified_at,
        geniallys.published_at,
        geniallys.last_view_at,
        geniallys.deleted_at,
        geniallys.disabled_at

    from geniallys
    inner join users
        on geniallys.user_id = users.user_id
)

select * from final
