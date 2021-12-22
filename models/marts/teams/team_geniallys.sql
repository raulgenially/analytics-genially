{{
  config(
    materialized='view'
  )
}}

with geniallys as (
    select * from {{ ref('geniallys') }}
),

spaces as (
  select * from {{ ref('stg_team_spaces') }}
),

final as (
    select
        geniallys.genially_id,

        geniallys.name,
        geniallys.source,
        geniallys.category,
        geniallys.template_type,
        geniallys.template_name,
        geniallys.collaboration_type,
        geniallys.user_plan,
        geniallys.user_sector,
        geniallys.user_broad_sector,
        geniallys.user_role,
        geniallys.user_broad_role,
        geniallys.user_country,
        spaces.team_name,

        geniallys.is_published,
        geniallys.is_active,
        geniallys.is_in_recyclebin,
        geniallys.is_logically_deleted,
        geniallys.is_deleted,
        geniallys.is_disabled,
        geniallys.is_private,
        geniallys.is_password_free,
        geniallys.is_in_social_profile,
        geniallys.is_reusable,
        geniallys.is_inspiration,
        geniallys.is_visualized_last_90_days,
        geniallys.is_visualized_last_60_days,
        geniallys.is_visualized_last_30_days,
        geniallys.is_collaborative,
        geniallys.is_created_before_registration,
        spaces.is_team_disabled,

        geniallys.user_id,
        geniallys.reused_from_id,
        geniallys.from_template_id,
        geniallys.team_id,
        geniallys.space_id,
        geniallys.team_template_id,
        geniallys.from_team_template_id,

        geniallys.modified_at,
        geniallys.created_at,
        geniallys.published_at,
        geniallys.last_view_at,
        geniallys.deleted_at,
        geniallys.disabled_at,
        spaces.team_created_at,

    from geniallys
    inner join spaces
        on geniallys.space_id = spaces.team_space_id -- Only retains geniallys from existing spaces
)

select * from final
