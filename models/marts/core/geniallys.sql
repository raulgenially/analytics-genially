with geniallys as (
    select * from {{ ref('stg_geniallys') }}
),

users as (
    select * from {{ ref('stg_users') }}
),

collaboratives as (
    select * from {{ ref('stg_collaboratives') }}
),

geniallys_collaboratives as (
    select
        distinct genially_id

    from collaboratives
),

final as (
    select
        geniallys.genially_id,

        geniallys.plan as genially_plan,
        geniallys.name,
        geniallys.source,
        geniallys.category,
        geniallys.template_type,
        geniallys.template_name,
        users.plan as user_plan,
        users.sector as user_sector,
        users.broad_sector as user_broad_sector,
        users.role as user_role,
        users.country as user_country,

        geniallys.is_published,
        geniallys.is_active,
        geniallys.is_in_recyclebin,
        geniallys.is_logically_deleted,
        geniallys.is_deleted,
        geniallys.is_private,
        geniallys.is_password_free,
        geniallys.is_in_social_profile,
        geniallys.is_reusable,
        geniallys.is_inspiration,
        geniallys.is_visualized_last_90_days,
        geniallys.is_visualized_last_60_days,
        geniallys.is_visualized_last_30_days,
        if(geniallys_collaboratives.genially_id is not null, true, false) as is_collaborative,
        -- In some cases creation date < registration date
        case
            when geniallys.created_at is null or users.registered_at is null
                then null
            else geniallys.created_at < users.registered_at
        end as is_created_before_registration,

        geniallys.user_id,
        geniallys.reused_from_id,
        geniallys.from_template_id,

        geniallys.modified_at,
        geniallys.created_at,
        geniallys.published_at,
        geniallys.last_view_at,
        geniallys.deleted_at,

    from geniallys
    inner join users
        on geniallys.user_id = users.user_id
    left join geniallys_collaboratives
        on geniallys.genially_id = geniallys_collaboratives.genially_id
)

select * from final
