with users as (
    select * from {{ ref('stg_users') }}
),

geniallys as (
    select * from {{ ref('stg_geniallys') }}
),

collaboratives as (
    select * from {{ ref('stg_collaboratives') }}
),

users_creations as (
    select
        geniallys.user_id,
        count(geniallys.genially_id) as n_total_creations,
        countif(geniallys.is_deleted = false) as n_active_creations,
        countif(geniallys.is_published = true) as n_published_creations,
        countif(
            geniallys.is_deleted = false
            and geniallys.is_published = true
        ) as n_active_published_creations,
        countif(geniallys.is_in_social_profile = true) as n_creations_in_social_profile,
        countif(
            geniallys.is_deleted = false
            and geniallys.is_in_social_profile = true
        ) as n_active_creations_in_social_profile,
        countif(
            geniallys.is_deleted = false
            and geniallys.is_published = true
            and collaboratives.genially_id is not null -- Check if the genially is collaborative.
        ) as n_active_collaborative_published_creations,
        countif(
            geniallys.is_deleted = false
            and geniallys.is_in_social_profile = true
            and geniallys.is_reusable = true
        ) as n_active_reusable_creations_in_social_profile,

        max(geniallys.is_visualized_last_90_days) as has_creation_visualized_last_90_days,
        max(geniallys.is_visualized_last_60_days) as has_creation_visualized_last_60_days,
        max(geniallys.is_visualized_last_30_days) as has_creation_visualized_last_30_days

    from geniallys
    left join collaboratives
        on geniallys.genially_id = collaboratives.genially_id
    group by 1
),

users_collaboratives_union as (
    select
        user_owner_id as user_id,
        max(is_published) as is_collaborator_of_published_creation,
        sum(0) as n_published_creations_as_collaborator, -- to prevent double-counting
        max(is_in_social_profile and is_owner_social_profile_active) as is_collaborator_of_creation_in_social_profile,
        max(is_visualized_last_90_days) as is_collaborator_of_creation_visualized_last_90_days,
        max(is_visualized_last_60_days) as is_collaborator_of_creation_visualized_last_60_days,
        max(is_visualized_last_30_days) as is_collaborator_of_creation_visualized_last_30_days,

    from collaboratives
    group by 1

    union all

    select
        user_id,
        max(is_published) as is_collaborator_of_published_creation,
        countif(is_published = true) as n_published_creations_as_collaborator,
        max(is_in_social_profile and is_owner_social_profile_active) as is_collaborator_of_creation_in_social_profile,
        max(is_visualized_last_90_days) as is_collaborator_of_creation_visualized_last_90_days,
        max(is_visualized_last_60_days) as is_collaborator_of_creation_visualized_last_60_days,
        max(is_visualized_last_30_days) as is_collaborator_of_creation_visualized_last_30_days,

    from collaboratives
    group by 1
),

users_collaboratives as (
    select
        user_id,
        max(is_collaborator_of_published_creation) as is_collaborator_of_published_creation,
        sum(n_published_creations_as_collaborator) as n_published_creations_as_collaborator,
        max(is_collaborator_of_creation_in_social_profile) as is_collaborator_of_creation_in_social_profile,
        max(is_collaborator_of_creation_visualized_last_90_days) as is_collaborator_of_creation_visualized_last_90_days,
        max(is_collaborator_of_creation_visualized_last_60_days) as is_collaborator_of_creation_visualized_last_60_days,
        max(is_collaborator_of_creation_visualized_last_30_days) as is_collaborator_of_creation_visualized_last_30_days,

    from users_collaboratives_union
    group by 1
),

final as (
    select
        users.user_id,

        users.plan,
        replace(users.sector, '{{ var('unknown') }}', '{{ var('not_selected') }}') as sector,
        replace(users.sector_category, '{{ var('unknown') }}', '{{ var('not_selected') }}') as sector_category,
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
        users.social_profile_name,

        coalesce(users_creations.n_total_creations, 0) as n_total_creations,
        coalesce(users_creations.n_active_creations, 0) as n_active_creations,
        coalesce(users_creations.n_published_creations, 0) as n_published_creations,
        coalesce(users_collaboratives.n_published_creations_as_collaborator, 0) as n_published_creations_as_collaborator,
        coalesce(users_creations.n_active_published_creations, 0) as n_active_published_creations,
        coalesce(users_creations.n_active_collaborative_published_creations, 0)
            as n_active_collaborative_published_creations,
        coalesce(users_creations.n_creations_in_social_profile, 0) as n_creations_in_social_profile,
        coalesce(users_creations.n_active_creations_in_social_profile, 0) as n_active_creations_in_social_profile,
        coalesce(users_creations.n_active_reusable_creations_in_social_profile, 0)
            as n_active_reusable_creations_in_social_profile,

        users.is_validated,
        users.is_social_profile_active,
        ifnull(has_creation_visualized_last_90_days, false) as has_creation_visualized_last_90_days,
        ifnull(has_creation_visualized_last_60_days, false) as has_creation_visualized_last_60_days,
        ifnull(has_creation_visualized_last_30_days, false) as has_creation_visualized_last_30_days,
        if(users_collaboratives.user_id is not null, true, false) as is_collaborator,
        ifnull(is_collaborator_of_published_creation, false) as is_collaborator_of_published_creation,
        ifnull(is_collaborator_of_creation_in_social_profile, false) as is_collaborator_of_creation_in_social_profile,
        ifnull(is_collaborator_of_creation_visualized_last_90_days, false) as is_collaborator_of_creation_visualized_last_90_days,
        ifnull(is_collaborator_of_creation_visualized_last_60_days, false) as is_collaborator_of_creation_visualized_last_60_days,
        ifnull(is_collaborator_of_creation_visualized_last_30_days, false) as is_collaborator_of_creation_visualized_last_30_days,

        users.registered_at,
        users.last_access_at

    from users
    left join users_creations
        on users.user_id = users_creations.user_id
    left join users_collaboratives
        on users.user_id = users_collaboratives.user_id
)

select * from final
