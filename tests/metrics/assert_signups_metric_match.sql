-- The number of signups should match among the implicated models.
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
    select * from {{ ref('metrics_users_and_creations_by_day') }}
),

users_in_funnel as (
    select * from {{ ref('metrics_users_in_funnel') }}
),

signups_model1 as (
    select
        sum(n_signups) as n_signups

    from login_activity_active_users
    where date_day >= {{ testing_date }}
),

signups_model2 as (
    select
        sum(n_signups) as n_signups

    from monthly_projections
    where date_day >= {{ testing_date }}
),

signups_model3 as (
    select
        sum(n_signups) as n_signups

    from users_in_funnel
    where registered_at >= {{ testing_date }}
),

signups_model4 as (
    select
        sum(n_signups) as n_signups

    from users_and_creations_by_day
    where date_day >= {{ testing_date }}
),

signups_union as (
    select n_signups from signups_model1
    union all
    select n_signups from signups_model2
    union all
    select n_signups from signups_model3
    union all
    select n_signups from signups_model4
),

signups_count as (
    select
        count(distinct n_signups) as n_values

    from signups_union
),

final as (
    select * from signups_count
    where n_values != 1
)

select * from final
