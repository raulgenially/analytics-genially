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
    select * from {{ ref('metrics_reporting_users_and_creations_by_day') }}
),

users_in_funnel as (
    select * from {{ ref('metrics_users_in_funnel') }}
),

login_activity_active_users_sum as (
    select
        sum(n_signups) as n_signups

    from login_activity_active_users
    where date_day >= {{ testing_date }}
),

monthly_projections_sum as (
    select
        sum(n_signups) as n_signups

    from monthly_projections
    where date_day >= {{ testing_date }}
),

users_in_funnel_sum as (
    select
        sum(n_signups) as n_signups

    from users_in_funnel
    where registered_at >= {{ testing_date }}
),

users_and_creations_by_day_sum as (
    select
        sum(n_signups) as n_signups

    from users_and_creations_by_day
    where date_day >= {{ testing_date }}
),

final as (
    {{ compare_metric_consistency_between_models(
        ctes = [
            'login_activity_active_users_sum', 
            'monthly_projections_sum', 
            'users_in_funnel_sum', 
            'users_and_creations_by_day_sum'
            ], 
        metric = 'n_signups'
    ) }}
)

select * from final
