{{ config(
    materialized='view'
) }}

with users as (
    select * from {{ ref('activated_users') }}
),

final as (
    select * from users
    where {{ define_active_user() }}
)

select * from final
