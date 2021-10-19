with old_role_codes as (
    select * from {{ ref('old_role_codes') }}
),

role_codes as (
    select * from {{ ref('role_codes') }}
),

final as (
    select
        old_role_codes.role_id as old_role_id,
        concat(old_role_codes.role_name, ' (old)') as old_role_name,
        old_role_codes.sector_id as old_sector_id,
        concat(old_role_codes.sector_name, ' (old)') as old_sector_name,
        old_role_codes.new_role_id,
        role_codes.role_name as new_role_name,
        role_codes.sector_id as new_sector_id,
        role_codes.sector_name as new_sector_name

    from old_role_codes
    left join role_codes
        on old_role_codes.new_role_id = role_codes.role_id
)

select * from final
