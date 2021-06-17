{{
    config(
        materialized='ephemeral'
    )
}}

with heavy_users as (
    select * 
    from {{ ref('dim_users') }}
    where n_total_creations >= 5
)

select * from heavy_users