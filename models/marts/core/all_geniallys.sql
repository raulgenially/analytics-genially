with geniallys as (
    select * from {{ ref('stg_geniallys') }}
),

all_users as (
    select * from {{ ref('int_mart_all_users') }}
),

final as (
    select
        geniallys.genially_id,

        all_users.plan as user_plan,
        all_users.subscription as user_subscription,
        all_users.sector as user_sector,
        all_users.broad_sector as user_broad_sector,
        all_users.role as user_role,
        all_users.broad_role as user_broad_role,
        all_users.country as user_country,
        all_users.country_name as user_country_name,

        geniallys.is_published,
        geniallys.is_active,

        geniallys.user_id,

        geniallys.created_at,
        geniallys.published_at,
        all_users.deleted_at as user_deleted_at

    from geniallys
    inner join all_users
        on geniallys.user_id = all_users.user_id
)

select * from final
