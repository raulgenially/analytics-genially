-- Fields has_creation_visualized_last_X_days should be false for users with 0 creations.

with users as (
  select * from {{ ref('users') }}
),

final as (
    select
        user_id,
        n_total_creations, 
        has_creation_visualized_last_90_days,
        has_creation_visualized_last_60_days,
        has_creation_visualized_last_30_days,

    from users
    where n_total_creations = 0
        and (has_creation_visualized_last_90_days = true
            or has_creation_visualized_last_60_days = true
            or has_creation_visualized_last_30_days = true)
)

select * from final
