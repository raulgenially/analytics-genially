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
        {{ parse_user_profile('newsector', 'sector_codes.sector_name') }} as sector,
        {{ parse_user_profile('newsector', 'sector_codes.category') }} as broad_sector,
        {{ parse_user_profile('newrole', 'role_codes.role_name') }} as role,
        username,
        nickname,
        lower(email) as email,
        {{ clean_country_code('country') }} as country,
        city,
        logins,
        language,
        organization,
        -- socialmedia extraction
        json_extract_scalar(socialmedia, '$.facebook') as facebook_account,
        json_extract_scalar(socialmedia, '$.twitter') as twitter_account,
        json_extract_scalar(socialmedia, '$.youtube') as youtube_account,
        json_extract_scalar(socialmedia, '$.instagram') as instagram_account,
        json_extract_scalar(socialmedia, '$.linkedin') as linkedin_account,
        -- emailvalidationtoken extraction
        json_extract_scalar(emailvalidationtoken, '$.Token') as email_validation_token,
        summary as about_me,

        ifnull(validated, False) as is_validated,

        idanalytics as analytics_id,

        -- First valid registration date is 2015-02-23T13:27:13 (as of 2021-07-15)
        if(dateregister >= '2015-02-23', dateregister, null) as registered_at,
        -- First valid last access date is 2016-06-02T17:01:47 (as of 2021-07-15)
        if(lastaccesstime >= '2016-06-02', lastaccesstime, null) as last_access_at,
        -- emailvalidationtoken extraction
        timestamp_millis(cast(json_extract_scalar(emailvalidationtoken, '$.CreatedAt') as int64)) as email_validation_created_at,

    from users
    left join sector_codes
        on users.newsector = sector_codes.sector_id
    left join role_codes
        on users.newrole = role_codes.role_id
    where __hevo__marked_deleted = false
        and email is not null
)

select * from final
