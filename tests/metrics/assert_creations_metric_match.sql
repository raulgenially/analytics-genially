-- The number of creations should match among the implicated models.
{% set testing_date %}
    date('2021-12-20')
{% endset %}

with login_activity_active_users as (
    select 
        n_creations

    from {{ ref('metrics_login_activity_active_users') }}
    where date_day >= {{ testing_date }}
),

monthly_projections as (
    select 
        n_creations 

    from {{ ref('metrics_monthly_projections') }}
    where date_day >= {{ testing_date }}
),

users_and_creations_by_day as (
    select 
        n_creations
        
    from {{ ref('metrics_reporting_users_and_creations_by_day') }}
    where date_day >= {{ testing_date }}
),

final as (
    {{ compare_metric_consistency_between_models(
        ctes = [
            'login_activity_active_users', 
            'monthly_projections', 
            'users_and_creations_by_day'
            ], 
        metric = 'n_creations'
    ) }}
)

select * from final
