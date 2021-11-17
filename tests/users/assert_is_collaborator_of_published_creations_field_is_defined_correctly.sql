-- If a user is collaborator of a published creation they should have some published creation. 
-- If not, number of published creations as collaborator should be zero.
with users as (
  select * from {{ ref('users') }}
),

final as (
    select
        user_id,
        is_in_collaboration_of_published_creation,
        n_published_creations_as_collaborator,
        n_published_creations,

    from users
    where (is_in_collaboration_of_published_creation = true
            and n_published_creations_as_collaborator = 0
            and n_published_creations = 0)
        or (is_in_collaboration_of_published_creation = false
            and n_published_creations_as_collaborator > 0)
)

select * from final
