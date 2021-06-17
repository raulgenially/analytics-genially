with final as (
    select
        user_id, 
        n_total_creations, 
        first_creation_at,
        last_creation_at

    from {{ ref('dim_users') }}
    where n_total_creations = 0 and 
        (first_creation_at is not null or last_creation_at is not null)
)

select * from final