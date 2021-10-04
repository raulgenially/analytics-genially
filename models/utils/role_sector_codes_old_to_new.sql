with role_sector_mapping as (
    select * from {{ ref('role_sector_mapping') }}
),

role_codes as (
    select * from {{ ref('role_codes') }}
),

final as (
    select
        role_sector_mapping.old_role_id,
        role_sector_mapping.old_role_name,
        role_sector_mapping.old_sector_id,
        role_sector_mapping.old_sector_name,
        role_sector_mapping.new_role_id,
        role_codes.role_name as new_role_name,
        role_codes.sector_id as new_sector_id,
        role_codes.sector_name as new_sector_name

    from role_sector_mapping
    left join role_codes
        on role_sector_mapping.new_role_id = role_codes.role_id
)

select * from final
