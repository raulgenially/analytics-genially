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
    where {{ define_active_user(active_period=90) }}
)

select * from final
