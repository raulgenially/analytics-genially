with users as (
    select * from {{ source('genially', 'users') }}
),

sector_codes as (
    select * from {{ ref('sector_codes') }}
),

role_codes as (
    select * from {{ ref('role_codes') }}
),

final as (
    select 
        _id as user_id,

        {{ map_subscription_code('typesubscription') }} as subscription_plan,
        coalesce(sector_codes.sector_name, 'Missing') as sector,	
        coalesce(role_codes.role_name, 'Missing') as role,	
        username,
        nickname,	
        email,
        case 
            when country = 'GB' 
                then 'UK' 
            when country = '' or country is null
                then 'Missing'
            else country
        end as country,
        city,	
        logins,	
        language,		
        organization,		
        socialmedia as social_media_accounts,
        summary,

        ifnull(validated, False) as is_validated,

        idanalytics	as analytics_id,
        
        dateregister as registered_at,
        ifnull(lastaccesstime, dateregister) as last_access_at

    from users
    left join sector_codes 
        on users.newsector = sector_codes.sector_id
    left join role_codes
        on users.newrole = role_codes.role_id
    where date(dateregister) >= date(2014, 1, 1) -- Remove users for testing
        and __hevo__marked_deleted = False
)

select * from final