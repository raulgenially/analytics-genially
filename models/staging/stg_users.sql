with users as (
    select * from {{ ref('src_genially_users') }}
),

social as (
    select * from {{ ref('src_genially_social') }}
),

final as (
    select
        users.user_id,

        users.subscription_plan as plan,
        users.sector_code,
        users.sector,
        users.role_code,
        users.role,
        users.country,
        users.email,
        users.language,
        users.about_me,
        users.facebook_account,
        users.twitter_account,
        users.youtube_account,
        users.instagram_account,
        users.linkedin_account,
        social.social_profile_name,

        users.is_validated,
        ifnull(social.is_active, false) as is_social_profile_active,

        users.registered_at,
        users.last_access_at

    from users
    left join social
        on users.user_id = social.user_id
)

select * from final
