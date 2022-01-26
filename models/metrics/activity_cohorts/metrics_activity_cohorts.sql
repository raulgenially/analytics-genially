--As a view, the billed size is less than a table.
--This is the view needed to make the activity cohorts.
{{
  config(
    materialized='view'
  )
}}

with logins as (
    select * from {{ ref('user_logins') }}
),

ga_signups as (
    select * from {{ ref('signup_events') }}
),

users as (
    select * from {{ ref('users') }}
),

user_usage as (
    select
        users.user_id,
        {{ place_main_dimension_fields('users') }},
        coalesce(ga_signups.device, '{{ var('unknown') }}') as device,
        coalesce(ga_signups.channel, '{{ var('unknown') }}') as acquisition_channel,
        date(users.registered_at) as registered_at

    from users
    left join ga_signups
        on users.user_id = ga_signups.user_id
),

user_usage_traffic as (
    select
        user_usage.user_id,
        {{ place_main_dimension_fields('user_usage') }},
        user_usage.device,
        user_usage.acquisition_channel,
        user_usage.registered_at,
        logins.login_at,
        date_diff(date(logins.login_at), (user_usage.registered_at), day) as n_days_since_first_usage

    from user_usage
    left join logins
        on user_usage.user_id = logins.user_id
        and logins.login_at is not null
),

final as (
    select
        user_id,
        {{ place_main_dimension_fields('user_usage_traffic') }},
        device,
        acquisition_channel,
        registered_at,
        login_at,
        n_days_since_first_usage

    from user_usage_traffic
)

select * from final
