with users as (
    select * from {{ ref('stg_genially_users') }}
),

sector_codes as (
    select * from {{ ref('sector_codes') }}
),

role_codes as (
    select * from {{ ref('role_codes') }}
),


final as (
    select
        users.user_id,

        {{ map_subscription_code('users.subscription_plan') }} as plan,
        coalesce(sector_codes.sector_name, 'Missing') as sector,
        coalesce(role_codes.role_name, 'Missing') as role,
        coalesce(users.country, 'Missing') as market,
        
        users.register_at,
        users.last_access_at

        from users
        left join sector_codes 
            on users.sector = sector_codes.sector_id
        left join role_codes
            on users.role = role_codes.role_id
        where DATE(users.register_at) >= DATE(2019, 1, 1)
)

select * from final