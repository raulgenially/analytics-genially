-- The number of active users or logins should match among the implicated models.

{% set testing_date %}
    date('2021-12-20')
{% endset %}

with login_activity_active_users as (
    select * from {{ ref('metrics_login_activity_active_users') }}
),

monthly_projections as (
    select * from {{ ref('metrics_monthly_projections') }}
),

active_users as (
    select
        -- Note that we're summing up non-unique users,
        -- but it doesn't matter for testing purposes.
        sum(n_daily_active_users) as n_daily_active_users

    from login_activity_active_users
    where date_day >= {{ testing_date }}
),

logins as (
    select
        sum(n_total_visitors) as n_logins

    from monthly_projections
    where date_day >= {{ testing_date }}
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
