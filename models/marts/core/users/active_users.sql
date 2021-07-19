{% set active_period = 90 %} -- in days

with users as (
    select * from {{ ref('activated_users') }}
),

final as (
    select * from users
    where last_access_at > timestamp_sub(current_timestamp(), interval {{ active_period }} day)
)

select * from final
