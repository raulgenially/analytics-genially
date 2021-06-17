with geniallys as (
    select * from {{ ref('stg_genially_geniallys') }}
),

users as (
    select * from {{ ref('dim_users') }}
),

final as (
    select
        geniallys.genially_id,

        geniallys.genially_type,
        users.plan,
        users.sector,
        users.role,
        users.market,

        geniallys.is_published,
        geniallys.is_deleted,

        geniallys.user_id,
        
        geniallys.modified_at,
        geniallys.created_at,
        geniallys.published_at,
        geniallys.last_view_at,
        geniallys.deleted_at

    from geniallys
    inner join users 
        on geniallys.user_id = users.user_id
    where DATE(geniallys.created_at) >= DATE(2019, 1, 1)
)

select * from final