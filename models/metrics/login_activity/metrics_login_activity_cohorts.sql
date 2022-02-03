--As a view, the billed size is less than a table.
--This is the view needed to make the activity cohorts.
{{
  config(
    materialized='view'
  )
}}

with activity as (
    select * from {{ ref('metrics_login_activity') }}
),

final as (
    select
        user_id,
        {{ place_main_dimension_fields('activity') }},
        signup_device,
        signup_channel,
        first_usage_at as registered_at,
        date_day as login_at,
        n_days_since_first_usage

    from activity
    where is_active = true
)

select * from final
