{% set not_select = 'Not-selected' %}

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
        coalesce(sector_codes.sector_name, '{{ not_select }}') as sector,
        coalesce(sector_codes.category, '{{ not_select }}') as sector_category,
        coalesce(role_codes.role_name, '{{ not_select }}') as role,
        username,
        nickname,
        lower(email) as email,
        case
            when country = 'GB'
                then 'UK'
            when country = '' or country is null
                then '{{ not_select }}'
            else country
        end as country,
        city,
        logins,
        language,
        organization,
        socialmedia as social_media_accounts,
        summary,

        ifnull(validated, False) as is_validated,

        idanalytics as analytics_id,

        -- First valid registration date is 2015-02-23T13:27:13 (as of 2021-07-15)
        if(dateregister >= '2015-02-23', dateregister, null) as registered_at,
        -- First valid last access date is 2016-06-02T17:01:47 (as of 2021-07-15)
        if(lastaccesstime >= '2016-06-02', lastaccesstime, null) as last_access_at

    from users
    left join sector_codes
        on users.newsector = sector_codes.sector_id
    left join role_codes
        on users.newrole = role_codes.role_id
    where __hevo__marked_deleted = false
        and email is not null
)

select * from final
