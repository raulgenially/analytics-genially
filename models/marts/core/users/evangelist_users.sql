with users as (
    select * from {{ ref('users') }}
),

final as (
    select * from users
    where {{ define_evangelist_user() }}
)

select * from final
