with users as (
    select * from {{ ref('src_genially_users') }}
),

social as (
    select * from {{ ref('src_genially_social') }}
),

sector_codes as (
    select * from {{ ref('sector_codes') }}
),

final as (
    select
        users.user_id,

        users.subscription_plan as plan,
        replace(users.sector, '{{ var('unknown') }}', '{{ var('not_selected') }}') as sector,
        coalesce(sector_codes.category, '{{ var('not_selected') }}') as broad_sector,
        replace(users.role, '{{ var('unknown') }}', '{{ var('not_selected') }}') as role,
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
    left join sector_codes
        on users.sector_code = sector_codes.sector_id
)

select * from final
