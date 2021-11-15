-- We need to materialize this model to avoid resources exceeded error in users model
{{
  config(
    materialized='table'
  )
}}

with geniallys as (
    select * from {{ ref('src_genially_geniallys') }}
),

templates as (
    select * from {{ ref('stg_templates') }}
),

inspiration as (
    select distinct(genially_id) from {{ ref('src_genially_inspiration') }}
),

teams as (
    select * from {{ ref('src_genially_teams') }}
),

unique_templates as (
    select *
    from (
        select
            *,
            row_number() over (partition by template_id) as seqnum
        from templates
    ) as x
    where seqnum = 1
),

genially_templates as (
    select distinct
        genially_id
    from templates

    union distinct

    select distinct
        genially_to_view_id as genially_id
    from templates
),

final as (
    select
        --- Genially fields
        geniallys.genially_id,

        geniallys.subscription_plan as plan,
        geniallys.name,
        teams.name as team_name,
        case
            when geniallys.reused_from_id is not null
                then
                    case
                        when inspiration.genially_id is null
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
        {{ map_genially_category('unique_templates.template_type', 'geniallys.genially_type') }} as category,
        unique_templates.template_type,
        unique_templates.name as template_name,

        geniallys.is_published,
        geniallys.is_active,
        geniallys.is_in_recyclebin,
        geniallys.is_logically_deleted,
        geniallys.is_deleted,
        geniallys.is_disabled,
        teams.is_disabled as is_team_disabled,
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
        geniallys.team_id,
        geniallys.space_id,
        geniallys.team_template_id,
        geniallys.from_team_template_id,

        geniallys.created_at,
        geniallys.modified_at,
        geniallys.published_at,
        geniallys.last_view_at,
        geniallys.deleted_at,
        geniallys.disabled_at,
        teams.created_at as team_created_at

    from geniallys
    left join teams
        on geniallys.team_id = teams.team_id
    left join unique_templates
        on geniallys.from_template_id = unique_templates.template_id
    left join inspiration
        on geniallys.reused_from_id = inspiration.genially_id
     -- Remove geniallys that are templates or template colors
    left join genially_templates
        on geniallys.genially_id = genially_templates.genially_id
    where genially_templates.genially_id is null
)

select * from final
