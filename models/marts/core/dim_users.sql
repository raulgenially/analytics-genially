with users as (
    select * from {{ ref('stg_genially_users') }}
),

geniallys as (
    select * from {{ ref('stg_genially_geniallys') }}
),

sector_codes as (
    select * from {{ ref('sector_codes') }}
),

role_codes as (
    select * from {{ ref('role_codes') }}
),

users_geniallys as (
    select
        users.user_id,
        count(users.user_id) as n_total_creations,
        countif(geniallys.is_deleted = False) as n_active_creations,
        min(geniallys.created_at) as first_creation_at,
        max(geniallys.created_at) as last_creation_at

    from users
    inner join geniallys
        on users.user_id = geniallys.user_id
    group by 1
),

final as (
    select
        users.user_id,

        users.subscription_plan as plan,
        coalesce(sector_codes.sector_name, 'Missing') as sector,
        coalesce(role_codes.role_name, 'Missing') as role,
        coalesce(users.country, 'Missing') as market,
        coalesce(users_geniallys.n_total_creations, 0) as n_total_creations,
        coalesce(users_geniallys.n_active_creations, 0) as n_active_creations,
        
        users_geniallys.first_creation_at,
        users_geniallys.last_creation_at,
        users.register_at,
        users.last_access_at

    from users
    left join sector_codes 
        on users.sector = sector_codes.sector_id
    left join role_codes
        on users.role = role_codes.role_id
    left join users_geniallys
        on users.user_id = users_geniallys.user_id
    where DATE(users.register_at) >= DATE(2019, 1, 1)
)

select * from final