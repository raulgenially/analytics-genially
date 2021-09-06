-- To check to what extent active users 90 days is a subset of publishers
-- In this case, we should expect active users that are not publishers (this is possible by definition, see macro)
-- Therefore, a warning is included
{{
  config(
    severity='warn'
  )
}}

with publishers as (
  select * from {{ ref('publishers') }}
),

active_users_90_days as (
    select * from {{ ref('active_users_90_days') }}
),

final as (
    select
        active_users_90_days.user_id as active_users_90_days_user_id,
        active_users_90_days.email,
        publishers.user_id as publishers_user_id,
        publishers.email

    from active_users_90_days
    left join publishers
        on active_users_90_days.user_id = publishers.user_id
    where publishers.user_id is null
)

select * from final
