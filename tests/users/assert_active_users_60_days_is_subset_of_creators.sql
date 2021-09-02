-- To ensure that active users is a subset of creators
-- In this case, we should expect active users that are not creators (this is possible by definition, see macro)
-- Therefore, a warning is included
{{
  config(
    severity='warn'
  )
}}

with creators as (
  select * from {{ ref('creators') }}
),

active_users_60_days as (
    select * from {{ ref('active_users_60_days') }}
),

final as (
    select
        active_users_60_days.user_id as active_users_60_days_user_id,
        active_users_60_days.email,
        creators.user_id as creators_user_id,
        creators.email

    from active_users_60_days
    left join creators
        on active_users_60_days.user_id = creators.user_id
    where creators.user_id is null
)

select * from final
