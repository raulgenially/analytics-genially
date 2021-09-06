-- To ensure that active_users_30_days is a subset of active_users_60_days (there are no active_users_30_days out of active_users_60_days)
with active_users_60_days as (
  select * from {{ ref('active_users_60_days') }}
),

active_users_30_days as (
    select * from {{ ref('active_users_30_days') }}
),

final as (
    select
        active_users_30_days.user_id as active_users_30_days_user_id,
        active_users_30_days.email,
        active_users_60_days.user_id as active_users_60_days_user_id,
        active_users_60_days.email

    from active_users_30_days
    left join active_users_60_days
        on active_users_30_days.user_id = active_users_60_days.user_id
    where active_users_60_days.user_id is null
)

select * from final
