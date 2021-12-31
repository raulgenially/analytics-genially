-- We need to materialize this model to avoid resources exceeded error in users model
{{
  config(
    materialized='table'
  )
}}

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
        agg_sector
    from profiles
),

base_users as (
    select
        users.*,
        sectors.sector_name as sector,
        sectors.agg_sector as agg_sector,
        roles.role_name as role,

    from users
    left join sectors
        on users.sector_code = sectors.sector_id
    left join profiles as roles
        on users.role_code = roles.role_id
),

int_users as (
    select
        users.*,
        -- Sector mapping
        ifnull(profiles.new_sector_id, users.sector_code) as final_sector_code,
        ifnull(
            ifnull(profiles.new_sector_name, users.sector), '{{ var('not_selected') }}'
        ) as final_sector,
        ifnull(
            ifnull(profiles.agg_sector, users.agg_sector), '{{ var('not_selected') }}'
        ) as final_broad_sector,
        -- Role mapping
        ifnull(profiles.new_role_id, users.role_code) as final_role_code,
        ifnull(
            ifnull(profiles.new_role_name, users.role), '{{ var('not_selected') }}'
        ) as final_role,

    from base_users as users
    left join profiles
        on users.sector_code = profiles.sector_id
            and users.role_code = profiles.role_id
),

final as (
    select
        users.user_id,

        users.subscription_plan as plan,
        users.final_sector_code as sector_code,
        users.final_sector as sector,
        users.final_broad_sector as broad_sector,
        users.final_role_code as role_code,
        users.final_role as role,
        {{ create_broad_role_field('users.final_role', 'users.final_broad_sector') }} as broad_role,
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
        social.social_profile_name,

        users.is_validated,
        ifnull(social.is_active, false) as is_social_profile_active,

        users.registered_at,
        users.last_access_at

    from int_users as users
    left join social
        on users.user_id = social.user_id
)

select * from final
