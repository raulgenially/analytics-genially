with final as (
    select
        user_id, 
        n_total_creations, 
        n_active_creations

    from {{ ref('dim_users') }}
    where n_active_creations > n_total_creations
)

select * from final