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

user_profiles as (
    select
        user_id,
        -- Sector mapping (I'm not using macros here to better understand what's going on)
        case
            when users.sector like '%(old)' -- This row refers to a user from the old onboarding
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
            when users.role like '%(old)'
                then
                    case
                        when role_sector_mapping.new_role_name is not null
                            then role_sector_mapping.new_role_name
                        else users.role
                    end
            else users.role
        end as role
    
    from users
    -- Perform the join using names rather than codes to mitigate some inconsistencies
    -- See tests/src_genially_users/assert_role_info_is_tied_to_the_expected_sector_info.sql
    left join role_sector_mapping
        on users.sector = role_sector_mapping.old_sector_name
            and users.role = role_sector_mapping.old_role_name
),

final as (
    select
        users.user_id,

        users.subscription_plan as plan,
        user_profiles.sector,
        user_profiles.role,
        -- Agg sector
        case
            when sector_codes.agg_sector is null -- It means users.sector is unknown or not selected
                then users.sector
            else sector_codes.agg_sector
        end as broad_sector,
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
    inner join user_profiles
        on users.user_id = user_profiles.user_id
    -- Perform the join using names rather than codes to mitigate some inconsistencies
    -- See tests/src_genially_users/assert_role_info_is_tied_to_the_expected_sector_info.sql
    left join sector_codes
        on users.sector = sector_codes.sector_name
)

select * from final
