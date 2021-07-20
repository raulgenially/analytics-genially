{{ config(
    materialized='view'
) }}

with users as (
    select * from {{ ref('users') }}
),

final as (
    select * from users
    where {{ define_activated_user() }}
)

select * from final
