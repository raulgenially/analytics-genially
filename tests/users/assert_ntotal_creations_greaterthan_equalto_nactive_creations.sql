-- The number of active creations (not deleted) shouldn't exceed the number of total creations.

with users as (
  select * from {{ ref('users') }}
),

final as (
    select
        user_id, 
        n_total_creations, 
        n_active_creations

    from {{ ref('users') }}
    where n_active_creations > n_total_creations
)

select * from final
