with new_role_codes as (
    select * from {{ ref('new_role_codes') }}
),

old_role_codes as (
    select
        role_id,
        concat(role_name, ' (old)') as role_name,
        sector_id
    
    from {{ ref('old_role_codes') }}
),

sector_codes as (
    select * from {{ ref('sector_codes') }}
),

roles_codes_unioned as (
    select * from new_role_codes
    union distinct
    select * from old_role_codes
),

final as (
    select 
        roles_codes_unioned.role_id,
        roles_codes_unioned.role_name,
        roles_codes_unioned.sector_id,
        sector_codes.sector_name,
        sector_codes.category as sector_category

    from roles_codes_unioned
    left join sector_codes
        on roles_codes_unioned.sector_id = sector_codes.sector_id
)

select * from final
