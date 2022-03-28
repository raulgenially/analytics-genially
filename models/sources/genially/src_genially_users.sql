{{
    config(
        materialized="table",
    )
}}

with source as (
    select * from {{ source('genially', 'users') }}
    where email is not null
),

users as (
    {{ create_base_user_model(source_cte='source') }}
),

final as (
    select
        user_id,

        subscription_plan,
        sector_code,
        role_code,
        username,
        nickname,
        email_lower as email,
        country_code as country,
        country_name,
        city,
        logins,
        language,
        organization,
        facebook_account,
        twitter_account,
        youtube_account,
        instagram_account,
        linkedin_account,
        email_validation_token,
        about_me,

        is_validated,
        __hevo__marked_deleted as is_deleted,

        analytics_id,

        registered_at,
        last_access_at,
        email_validation_created_at,
        timestamp_millis(__hevo__ingested_at) as updated_at,

    from users
    where __hevo__marked_deleted = false
        or (
            __hevo__marked_deleted = true
            and registered_at is not null
        )
)

select * from final
