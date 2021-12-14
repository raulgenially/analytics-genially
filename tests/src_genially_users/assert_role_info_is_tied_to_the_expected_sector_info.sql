-- The user sector should match with the expected sector
{{
  config(
    severity='warn'
  )
}}

with users as (
    select * from {{ ref('src_genially_users') }}
),

role_codes as (
    select * from {{ ref('util_role_codes') }}
),

final as (
    select
        users.user_id,
        users.role_code as actual_role_code,
        users.sector_code as actual_sector_code,
        role_codes.sector_id as expected_sector_code,
        users.registered_at

    from users
    left join role_codes
        on users.role_code = role_codes.role_id
    where users.sector_code != role_codes.sector_id
    order by registered_at desc
)

select * from final
