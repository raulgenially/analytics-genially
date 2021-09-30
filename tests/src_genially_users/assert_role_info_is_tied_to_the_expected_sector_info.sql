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
    select * from {{ ref('role_codes') }}
),

final as (
    select
        users.user_id,
        users.role_code as actual_role_code, 
        users.role as actual_role_name,
        users.sector_code as actual_sector_code,
        users.sector as actual_sector_name,
        role_codes.sector_id as expected_sector_code,
        role_codes.sector_name as expected_sector_name,
        users.registered_at

    from users
    left join role_codes
        on users.role_code = role_codes.role_id
    where users.sector_code != role_codes.sector_id
)

select * from final
