with users as (
    select * from {{ ref('premium_active_users') }}
),

final as (
    select * from users
    where {{ evangelist_clause() }}
)

select * from final
