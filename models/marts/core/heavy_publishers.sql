{{
  config(
    materialized='view'
  )
}}

with users as (
    select * from {{ ref('users') }}
),

final as (
    select * from users
    where is_heavy_publisher = true
)

select * from final
