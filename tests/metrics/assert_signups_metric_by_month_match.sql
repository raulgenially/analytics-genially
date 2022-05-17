{% set testing_date %}
    date('2022-01-01')
{% endset %}

with users_and_creations_by_day as (
    select * from {{ ref('metrics_reporting_users_and_creations_by_day') }}
),

active_users_by_month as (
    select * from {{ ref('metrics_reporting_users_and_creations_by_month') }}
),

users_and_creations_by_day_sum as (
    select 
        sum(n_signups) as n_signups

    from users_and_creations_by_day
    where date_day >= {{ testing_date }}
        and date_day < {{ get_first_day_current_month() }}

),

active_users_by_month_sum as (
    select 
        sum(n_signups) as n_signups

    from active_users_by_month
    where date_month >= {{ testing_date }}

),

final as (
    {{ compare_metric_consistency_between_models(
        ctes = [
            'users_and_creations_by_day_sum', 
            'active_users_by_month_sum'
            ], 
        metric = 'n_signups'
    ) }}
)

select * from final
