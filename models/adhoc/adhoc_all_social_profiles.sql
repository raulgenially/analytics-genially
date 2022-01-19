with users as (
    select * from {{ ref('users') }}
    where social_profile_name is not null
),

geniallys as (
    select * from {{ ref('geniallys') }}
),

creations as (
    select
        user_id,
        count(genially_id) as geniallys_at_social_profile,
        countif(is_reusable) as reusable_geniallys_at_social_profile

    from geniallys
    where is_in_social_profile = true
        and is_active = true
    group by 1
),

final as (
    select
        users.email as UserName,
        concat("https://view.genial.ly/profile/", users.social_profile_name) as URL,
        users.is_social_profile_active as active,
        users.sector,
        users.country,
        users.language,
        users.twitter_account,
        users.facebook_account,
        users.youtube_account,
        users.instagram_account,
        users.linkedin_account,
        users.about_me,
        coalesce(creations.geniallys_at_social_profile, 0) as geniallys_at_social_profile,
        coalesce(creations.reusable_geniallys_at_social_profile, 0) as reusable_geniallys_at_social_profile

    from users
    left join creations
        on users.user_id = creations.user_id
    order by email asc
)

select * from final
