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

        case 
            when users.subscription_plan = 1
                then 'Free'
            when users.subscription_plan = 2
                then 'Pro'
            when users.subscription_plan = 3
                then 'Master'
            when users.subscription_plan = 4
                then 'Edu Pro'
            when users.subscription_plan = 5
                then 'Edu Team'
            when users.subscription_plan = 6
                then 'Team'
            when users.subscription_plan = 7
                then 'Student'
        end as plan,
        coalesce(sector_codes.sector_name, 'Missing') as sector,
        coalesce(role_codes.role_name, 'Missing') as role,
        coalesce(users.country, 'Missing') as market,
        
        users.register_at

        from users
        left join sector_codes 
            on users.sector = sector_codes.sector_id
        left join role_codes
            on users.role = role_codes.role_id
        where DATE(users.register_at) >= DATE(2019, 1, 1)
)

select * from final