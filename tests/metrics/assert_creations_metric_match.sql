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

login_activity_active_users_sum as (
    select
        sum(n_creations) as n_creations

    from login_activity_active_users
    where date_day >= {{ testing_date }}
),

monthly_projections_sum as (
    select
        sum(n_creations) as n_creations

    from monthly_projections
    where date_day >= {{ testing_date }}
),

users_and_creations_by_day_sum as (
    select
        sum(n_creations) as n_creations

    from users_and_creations_by_day
    where date_day >= {{ testing_date }}
),

final as (
    {{ compare_metric_consistency_between_models(
        ctes = [
            'login_activity_active_users_sum', 
            'monthly_projections_sum', 
            'users_and_creations_by_day_sum'
            ], 
        metric = 'n_creations'
    ) }}
)

select * from final
