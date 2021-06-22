-- When the number of creations is 0, first and last creations dates should be NULL.

with final as (
    select
        user_id, 
        n_total_creations, 
        first_creation_at,
        last_creation_at

    from {{ ref('users') }}
    where n_total_creations = 0 and 
        (first_creation_at is not null or last_creation_at is not null)
)

select * from final