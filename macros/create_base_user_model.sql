{% macro create_base_user_model(source_name=null, source_table=null) %}

with users as (
    select * from {{ source(source_name, source_table) }}
    {% if source_name == 'snapshots' %}
        where email is not null
    {% else %}
        where __hevo__marked_deleted = false
            and email is not null
    {% endif %}
),

subscription_plans as (
    select * from {{ ref('seed_plan') }}
),

-- There are some cases in which dateregister > lastaccesstime by a few seconds
-- We assume these cases to be just a sync issue during the auth process.
-- Here we fix these discrepancies by syncing lastaccesstime to dateregister if
-- the time difference is behind a certain threshold.
int_users as (
    select
        *,
        -- We take dateregister as the true date
        if(
            dateregister > lastaccesstime
            and abs(date_diff(dateregister, lastaccesstime, SECOND)) < 10,
            dateregister,
            lastaccesstime
        ) as synced_lastaccesstime,
    from users
),

final as (
    select
        *,
        _id as user_id,

        subscription_plans.plan as subscription_plan,
        newsector as sector_code,
        newrole as role_code,
        lower(email) as email_lower,
        {{ clean_country_code('country') }} as country_code,
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
        if(synced_lastaccesstime >= '2016-06-02', synced_lastaccesstime, null) as last_access_at,
        -- emailvalidationtoken extraction
        timestamp_millis(cast(json_extract_scalar(emailvalidationtoken, '$.CreatedAt') as int64)) as email_validation_created_at,

    from int_users
    left join subscription_plans
        on int_users.typesubscription = subscription_plans.code

)

select * from final

{% endmacro %}
