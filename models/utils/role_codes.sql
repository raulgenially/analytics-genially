with new_role_codes as (
    select * from {{ ref('new_role_codes') }}
),

old_role_codes as (
    select * from {{ ref('old_role_codes') }}
),

final as (
    select * from new_role_codes
    union distinct
    select * from old_role_codes
)

select * from final