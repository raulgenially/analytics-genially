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

role_sector_mapping as (
    select * from {{ ref('role_sector_codes_old_to_new') }}
),

sector_codes as (
    select * from {{ ref('sector_codes') }}
),

users_new_profiles as (
    select
        users.user_id,
        -- Sector mapping (I'm not using macros here to better understand what's going on)
        case
            when users.sector_code < 200 -- This row refers to a user from the old onboarding
                then
                    case
                        when role_sector_mapping.new_sector_id is not null
                            then role_sector_mapping.new_sector_id
                        else users.sector_code
                    end
            else users.sector_code
        end as sector_code,
        case
            when users.sector_code < 200 -- This row refers to a user from the old onboarding
                then
                    case
                        when role_sector_mapping.new_sector_name is not null
                            then role_sector_mapping.new_sector_name
                        else users.sector
                    end
            else users.sector
        end as sector,
        -- Role mapping (similar to above)
        case
            when users.role_code < 100
                then
                    case
                        when role_sector_mapping.new_role_id is not null
                            then role_sector_mapping.new_role_id
                        else users.role_code
                    end
            else users.role_code
        end as role_code,
        case
            when users.role_code < 100
                then
                    case
                        when role_sector_mapping.new_role_name is not null
                            then role_sector_mapping.new_role_name
                        else users.role
                    end
            else users.role
        end as role
    
    from users
    left join role_sector_mapping
        on users.sector_code = role_sector_mapping.old_sector_id
            and users.role_code = role_sector_mapping.old_role_id
),

users_broad_sector as (
    select
        users_new_profiles.user_id,
        users_new_profiles.sector_code,
        users_new_profiles.sector,
        users_new_profiles.role_code,
        users_new_profiles.role,
        if(sector_codes.agg_sector is null, '{{ var('not_selected') }}', sector_codes.agg_sector) as broad_sector,

    from users_new_profiles
    left join sector_codes
        on users_new_profiles.sector_code = sector_codes.sector_id
),

final as (
    select
        users.user_id,

        users.subscription_plan as plan,
        users_broad_sector.sector_code,
        users_broad_sector.sector,
        users_broad_sector.broad_sector,
        users_broad_sector.role_code,
        users_broad_sector.role,
        {{ create_broad_role_field('users_broad_sector.role', 'users_broad_sector.broad_sector') }} as broad_role,
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
    inner join users_broad_sector
        on users.user_id = users_broad_sector.user_id
    left join social
        on users.user_id = social.user_id
)

select * from final
