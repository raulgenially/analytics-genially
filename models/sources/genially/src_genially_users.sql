with users as (
    {{ create_base_user_model(src='users') }}
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

        analytics_id,

        registered_at,
        last_access_at,
        email_validation_created_at,

    from users
)

select * from final
