with heavy_users as (
    select * 
    from {{ ref('users') }}
    where n_total_creations >= 5
)

select * from heavy_users