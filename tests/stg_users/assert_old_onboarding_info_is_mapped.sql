-- We should not see user profiles related to the old onboarding after the mapping (because that is the aim of the mappin :))
-- If so, this is likely to be a consequence of tests/src_genially_users/assert_role_info_is_tied_to_the_expected_sector_info.sql
{{
  config(
    severity='warn' 
  )
}}

with users as (
    select * from {{ ref('stg_users') }}
),

user_profiles as (
    select * from {{ ref('user_profiles') }}
),

final as (
    select
        users.user_id,
        users.sector_code,
        users.sector,
        users.role_code,
        users.role,
        users.registered_at,
        user_profiles.*,

    from users
    left join user_profiles
        on user_profiles.role_id = users.role_code

    where (users.sector_code < 200 and users.role_code < 100)
          and users.sector_code = user_profiles.sector_id
    order by registered_at desc
)

select * from final
