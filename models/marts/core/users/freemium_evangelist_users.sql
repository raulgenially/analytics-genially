with users as (
    select * from {{ ref('freemium_active_users') }}
),

final as (
    select * from users
    where {{ evangelist_clause() }}
)

select * from final
