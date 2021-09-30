-- Ensure a role code is associated with a known sector
with role_codes as (
  select * from {{ ref('role_codes') }}
),

final as (
    select
        *
    from role_codes
    where sector_name is null
)

select * from final
