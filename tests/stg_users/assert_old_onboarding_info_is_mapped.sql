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

final as (
    select
        user_id,
        sector,
        role,
        registered_at

    from users
    where (sector != '{{ var('not_selected') }}'
            and role != '{{ var('not_selected') }}')
          and (sector_code < 200
              or role_code < 100)
    order by registered_at desc
)

select * from final
