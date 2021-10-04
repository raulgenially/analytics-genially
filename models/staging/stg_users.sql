with users as (
    select * from {{ ref('src_genially_users') }}
),

social as (
    select * from {{ ref('src_genially_social') }}
),

role_sector_mapping as (
    select * from {{ ref('role_sector_codes_old_to_new') }}
),

final as (
    select
        users.user_id,

        users.subscription_plan as plan,
        -- Sector mapping
        case
            when users.sector like '%(old)' -- This row refers to a user from the old onboarding
                then
                    case
                        when users.role = '{{ var('not_selected') }}' -- Let's be harsh with the mapping
                            then '{{ var('not_selected') }}'
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
                        when users.sector = '{{ var('not_selected') }}'
                            then '{{ var('not_selected') }}'
                        when role_sector_mapping.new_role_name is not null
                            then role_sector_mapping.new_role_name
                        else users.role
                    end
            else users.role
        end as role,
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
    -- Perform the join using names rather than codes to mitigate some inconsistencies
    -- See tests/src_genially_users/assert_role_info_is_tied_to_the_expected_sector_info.sql
    left join role_sector_mapping
        on users.sector = role_sector_mapping.old_sector_name
            and users.role = role_sector_mapping.old_role_name
)

select * from final
