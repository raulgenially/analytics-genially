-- Fields related to collaboratives should make sense

with users as (
  select * from {{ ref('users') }}
),

final as (
    select
        user_id,
        is_collaborator,
        is_collaborator_of_creation_in_social_profile,
        is_collaborator_of_creation_visualized_last_90_days,
        is_collaborator_of_creation_visualized_last_60_days,
        is_collaborator_of_creation_visualized_last_30_days

    from users
    where is_collaborator = false
        and (is_collaborator_of_creation_in_social_profile = true
            or is_collaborator_of_creation_visualized_last_90_days = true
            or is_collaborator_of_creation_visualized_last_60_days = true
            or is_collaborator_of_creation_visualized_last_30_days = true)
)

select * from final
