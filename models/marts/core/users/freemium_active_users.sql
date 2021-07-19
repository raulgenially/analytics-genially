with users as (
    select * from {{ ref('active_users') }}
),

final as (
    select * from users
    where plan = 'Free'
)

select * from final
