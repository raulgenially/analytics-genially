with users as (
    select * from {{ ref('stg_users') }}
    where is_deleted = true
),

phishing as (
    select * from {{ ref('src_miscellaneous_phishing_users') }}
),

users_cleaned as (
    select
        users.*

    from users
    left join phishing
        on users.user_id = phishing.user_id
    where phishing.user_id is null
),

final as (
    select
        user_id,

        plan,
        sector_code,
        sector,
        broad_sector,
        role_code,
        role,
        broad_role,
        country,
        country_name,
        email,
        nickname,
        language,
        about_me,
        facebook_account,
        twitter_account,
        youtube_account,
        instagram_account,
        linkedin_account,

        is_validated,

        registered_at,
        last_access_at,
        updated_at as deleted_at

    from users_cleaned
)

select * from final
