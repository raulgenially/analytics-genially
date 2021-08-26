with geniallys as (
    select * from {{ ref('stg_geniallys') }}
),

users as (
    select * from {{ ref('stg_users') }}
),

collaboratives as (
    select * from {{ ref('stg_collaboratives') }}
),

final as (
    select
        geniallys.genially_id,

        geniallys.plan as genially_plan,
        geniallys.origin,
        geniallys.category,

        geniallys.is_published,
        geniallys.is_deleted,
        geniallys.is_private,
        geniallys.is_password_free,
        geniallys.is_in_social_profile,
        geniallys.is_reusable,
        geniallys.is_inspiration,
        if(collaboratives.genially_id is not null, true, false) as is_collaborative,
        -- In some cases creation date < registration date
        case
            when geniallys.created_at is null or users.registered_at is null
                then null
            else geniallys.created_at < users.registered_at
        end as is_created_before_registration,

        geniallys.user_id,
        if(users.user_id is not null, true, false) as is_from_current_user,
        geniallys.reused_from_id,
        geniallys.from_template_id,
        geniallys.template_type,
        geniallys.template_name,

        geniallys.modified_at,
        geniallys.created_at,
        geniallys.published_at,
        geniallys.last_view_at,
        geniallys.deleted_at,

        users.plan as user_plan,
        users.sector as user_sector,
        users.role as user_role,
        users.market as user_market

    from geniallys
    left join users
        on geniallys.user_id = users.user_id
    left join collaboratives
        on geniallys.genially_id = collaboratives.genially_id
)

select * from final
