with users as (
    select 
        _id as user_id,
        typesubscription as subscription_type,
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
        validated as is_validated,	
        language,		
        organization,		
        socialmedia as social_media_accounts,
        summary,
        idanalytics	as analytics_id,
        dateregister as register_at,	
        lastaccesstime as last_access_at

    from {{ source('genially', 'users') }}
    where dateregister is not null and lastaccesstime is not null -- remove very old users
)

select * from users