-- The number of active users or logins should match among the implicated models.
with login_activity_active_users as (
    select * from {{ ref('metrics_login_activity_active_users') }}
),

monthly_projections as (
    select * from {{ ref('metrics_monthly_projections') }}
),

active_users as (
    select
        date_day,
        plan,
        country,
        sum(n_daily_active_users) as n_daily_active_users

    from login_activity_active_users
    where date_day = '2022-01-25'
        and plan = 'Free'
        and country = 'FR'
    {{ dbt_utils.group_by(n=3) }}
),

logins as (
    select
        date_day,
        plan,
        country,
        sum(n_total_visitors) as n_logins

    from monthly_projections
    where date_day = '2022-01-25'
        and plan = 'Free'
        and country = 'FR'
    {{ dbt_utils.group_by(n=3) }}
),

final as (
    select
        active_users.n_daily_active_users,
        logins.n_logins

    from active_users
    cross join logins
    where active_users.n_daily_active_users != logins.n_logins
)

select * from final
