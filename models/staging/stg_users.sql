with users as (
    select * from {{ ref('src_genially_users') }}
),

social as (
    select * from {{ ref('src_genially_social') }}
),

profiles as (
    select * from {{ ref('util_user_profiles') }}
),

sectors as (
    select distinct
        sector_id,
        sector_name,
        broad_sector
    from profiles
),

-- In this cte, users columns sector_code and role_code are mapped with its
-- corresponding descriptions - sector, broad_sector, role, broad_role
base_users as (
    select
        users.*,
        sectors.sector_name as sector,
        sectors.broad_sector as broad_sector,
        roles.role_name as role,
        roles.broad_role as broad_role

    from users
    left join sectors
        on users.sector_code = sectors.sector_id
    left join profiles as roles
        on users.role_code = roles.role_id
),

-- Here we replace the codes and descriptions of sector and role columns:
-- 1. If the code is old (version 1), it is mapped with the new code (version 2)
-- 2. If the code is already new, or it is old but the mapping is not found,
--    then value from users is kept
-- 3. If the column is a description column and the mapping code-description
--    is not found, then it sets a default description 'Not-selected'
int_users as (
    select
        users.* replace(
            ifnull(profiles.new_sector_id, users.sector_code) as sector_code,
            coalesce(profiles.new_sector_name, users.sector,
                '{{ var('not_selected') }}') as sector,
            coalesce(profiles.broad_sector, users.broad_sector,
                '{{ var('not_selected') }}') as broad_sector,
            ifnull(profiles.new_role_id, users.role_code) as role_code,
            coalesce(profiles.new_role_name, users.role,
                '{{ var('not_selected') }}') as role,
            coalesce(profiles.broad_role, users.broad_role,
                '{{ var('not_selected') }}') as broad_role
        )

    from base_users as users
    left join profiles
        on users.sector_code = profiles.sector_id
            and users.role_code = profiles.role_id
),

final as (
    select
        users.user_id,

        users.plan,
        users.subscription,
        users.sector_code,
        users.sector,
        users.broad_sector,
        users.role_code,
        users.role,
        users.broad_role,
        users.country,
        users.country_name,
        users.email,
        users.nickname,
        users.language,
        users.organization_name,
        users.about_me,
        users.facebook_account,
        users.twitter_account,
        users.youtube_account,
        users.instagram_account,
        users.linkedin_account,
        social.social_profile_name,

        users.is_validated,
        users.is_deleted,
        ifnull(social.is_active, false) as is_social_profile_active,

        users.organization_id,

        users.registered_at,
        users.last_access_at,
        users.updated_at

    from int_users as users
    left join social
        on users.user_id = social.user_id
)

select * from final
