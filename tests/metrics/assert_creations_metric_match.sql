-- The number of creations should match among the implicated models.
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

creations_model1 as (
    select
        sum(n_creations) as n_creations

    from login_activity_active_users
    where date_day >= {{ testing_date }}
),

creations_model2 as (
    select
        sum(n_creations) as n_creations

    from monthly_projections
    where date_day >= {{ testing_date }}
),

creations_model3 as (
    select
        sum(n_creations) as n_creations

    from users_and_creations_by_day
    where date_day >= {{ testing_date }}
),

creations_union as (
    select n_creations from creations_model1
    union all
    select n_creations from creations_model2
    union all
    select n_creations from creations_model3
),

creations_count as (
    select
        count(distinct n_creations) as n_values

    from creations_union
),

totals_join as (
    select
        m1.n_creations as n_creations_loging_activity,
        m2.n_creations as n_creations_monthly_projections,
        m3.n_creations as n_creations_users_and_creations_by_day,
        c.n_values

    from creations_model1 as m1
    cross join creations_model2 as m2
    cross join creations_model3 as m3
    cross join creations_count as c
),

final as (
    select * from totals_join
    where n_values != 1
)

select * from final
