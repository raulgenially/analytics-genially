with users as (
    select 
        -- Primary key
        _id as user_id,

        -- User data
        typesubscription as subscription_plan,
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

        -- Foreign keys
        idanalytics	as analytics_id,
        
        -- Metadata
        validated as is_validated,	
        dateregister as register_at,	
        lastaccesstime as last_access_at

    from {{ source('genially', 'users') }}
    where dateregister is not null and lastaccesstime is not null -- Remove very old users
)

select * from users