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
    where is_creator = true
)

select * from final
