with users as (
    select 
        _id as user_id,
        typesubscription as subscription_type,	
        username,
        nickname,	
        email,
        country,
        city,	
        newsector as sector,	
        newrole	as role,
        logins,	
	    emailvalidationtoken as email_validation_token,
        validated as is_validated,	
        language,		
        organization,		
        socialmedia as social_media,
        summary,
        idanalytics	as analytics_id,
        dateregister as register_at,	
        lastaccesstime as last_access_at

    from {{ source('genially', 'users') }}
)

select * from users