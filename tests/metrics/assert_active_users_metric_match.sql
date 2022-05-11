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

users_and_creations_by_day as (
    select * from {{ ref('metrics_reporting_users_and_creations_by_day') }}
),

active_users_model1 as (
    select
        -- Note that we're summing up non-unique users,
        -- but it doesn't matter for testing purposes.
        sum(n_daily_active_users) as n_active_users

    from login_activity_active_users
    where date_day >= {{ testing_date }}
),

active_users_model2 as (
    select
        sum(n_total_visitors) as n_active_users

    from monthly_projections
    where date_day >= {{ testing_date }}
),

active_users_model3 as (
    select
        sum(n_active_users) as n_active_users

    from users_and_creations_by_day
    where date_day >= {{ testing_date }}
),

active_users_union as (
    select n_active_users from active_users_model1
    union all
    select n_active_users from active_users_model2
    union all
    select n_active_users from active_users_model3
),

active_users_count as (
    select
        count(distinct n_active_users) as n_values

    from active_users_union
),

totals_join as (
    select
        m1.n_active_users as n_active_users_loging_activity,
        m2.n_active_users as n_active_users_monthly_projections,
        m3.n_active_users as n_active_users_users_and_creations_by_day,
        c.n_values

    from active_users_model1 as m1
    cross join active_users_model2 as m2
    cross join active_users_model3 as m3
    cross join active_users_count as c
),

final as (
    select * from totals_join
    where n_values != 1
)

select * from final
