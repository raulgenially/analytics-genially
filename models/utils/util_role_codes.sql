with new_role_codes as (
    select * from {{ ref('seed_new_role_codes') }}
),

old_role_codes as (
    select
        role_id,
        concat(role_name, ' (old)') as role_name,
        sector_id

    from {{ ref('seed_old_role_codes') }}
),

final as (
    select * from new_role_codes
    union distinct
    select * from old_role_codes
)

select * from final
