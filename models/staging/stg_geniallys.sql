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

genially_templates as (
    select distinct
        genially_id
    from templates

    union distinct

    select distinct
        genially_to_view_id as genially_id
    from templates
),

base_geniallys as (
    select
        geniallys.*
    from geniallys
     -- Remove geniallys that are templates or template colors
    left join genially_templates
        on geniallys.genially_id = genially_templates.genially_id
    where genially_templates.genially_id is null
),

unique_templates as (
    select *
    from (
        select
            *,
            row_number() over (partition by template_id) as seqnum
        from templates
    )
    where seqnum = 1
),

unique_template_geniallys as (
    select *
    from (
        select
            *,
            row_number() over (partition by genially_id) as seqnum
        from templates
    )
    where seqnum = 1
),

int_geniallys as (
    select
        base_geniallys.*,
        ifnull(unique_templates.template_type, unique_template_geniallys.template_type) as template_type,
        ifnull(unique_templates.name, unique_template_geniallys.name) as template_name,
        ifnull(unique_templates.is_premium, unique_template_geniallys.is_premium) as is_from_premium_template,
        ifnull(unique_templates.language, unique_template_geniallys.language) as template_language,
        ifnull(unique_templates.genially_to_view_id, unique_template_geniallys.genially_to_view_id) as template_to_view_id

    from base_geniallys
    -- Some geniallys.from_template_id point to a genially_id instead of a template_id
    -- See https://github.com/Genially/scrum-genially/issues/7846#issuecomment-972727124
    -- Extract template information from either of the matches
    left join unique_templates
        on base_geniallys.from_template_id = unique_templates.template_id
    left join unique_template_geniallys
        on base_geniallys.from_template_id = unique_template_geniallys.genially_id
),

final as (
    select
        --- Genially fields
        geniallys.genially_id,

        geniallys.name,
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
        {{ map_genially_category('geniallys.template_type', 'geniallys.genially_type') }} as category,
        geniallys.template_type,
        geniallys.template_name,
        geniallys.template_language,

        geniallys.is_from_premium_template,
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
        geniallys.template_to_view_id,

        geniallys.created_at,
        geniallys.modified_at,
        geniallys.published_at,
        geniallys.last_view_at,
        geniallys.deleted_at,
        geniallys.disabled_at,

    from int_geniallys as geniallys
    left join inspiration
        on geniallys.reused_from_id = inspiration.genially_id
)

select * from final
