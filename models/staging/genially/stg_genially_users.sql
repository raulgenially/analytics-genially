with users as (
    select 
        _id as user_id,

        {{ map_subscription_code('typesubscription') }} as subscription_plan,
        newsector as sector,	
        newrole	as role,	
        username,
        nickname,	
        email,
        case 
            when country = 'GB' 
                then 'UK' 
            when country = ''
                then null
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
        
        dateregister as register_at,
        ifnull(lastaccesstime, dateregister) as last_access_at

    from {{ source('genially', 'users') }}
    where DATE(dateregister) >= DATE(2015, 1, 1) -- Remove very old users
)

select * from users