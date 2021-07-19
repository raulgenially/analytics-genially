{% set min_creations = 1 %}

with users as (
    select * from {{ ref('users') }}
),

final as (
    select * from users
    where users.n_total_creations > {{ min_creations }}
)

select * from final
