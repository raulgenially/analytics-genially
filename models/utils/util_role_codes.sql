with role_codes as (
    select * from {{ ref('seed_role_codes') }}
),

final as (
    select
        role_id,
        case
            when version = 1 then concat(role_name, ' (old)')
            else role_name
        end as role_name,
        sector_id

    from role_codes
)

select * from final
