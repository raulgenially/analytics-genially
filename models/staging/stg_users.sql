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
        users.sector,
        users.role,
        users.country,
        users.email,
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
