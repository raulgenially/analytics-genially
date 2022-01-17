with users as (
    select * from {{ ref('stg_users') }}
),

creations as (
    select * from {{ ref('int_mart_creations_by_user') }}
),

final as (
    select
        users.user_id,

        users.plan,
        {{ create_subscription_field('users.plan') }} as subscription,
        users.sector,
        users.broad_sector,
        users.role,
        users.broad_role,
        users.country,
        users.country_name,
        users.email,
        users.nickname,
        users.language,
        users.about_me,
        users.facebook_account,
        users.twitter_account,
        users.youtube_account,
        users.instagram_account,
        users.linkedin_account,
        users.social_profile_name,
        coalesce(creations.n_total_creations, 0) as n_total_creations,
        coalesce(creations.n_active_creations, 0) as n_active_creations,

        users.is_validated,
        users.is_social_profile_active,
        ifnull(creations.is_collaborator, false) as is_collaborator,
        ifnull({{ define_creator() }}, false) as is_creator,
        ifnull({{ define_publisher() }}, false) as is_publisher,
        ifnull({{ define_heavy_publisher() }}, false) as is_heavy_publisher,
        ifnull({{ define_recurrent_publisher() }}, false) as is_recurrent_publisher,

        users.registered_at,
        users.last_access_at

    from users
    left join creations
        on users.user_id = creations.user_id
)

select * from final
