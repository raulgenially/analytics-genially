{% set testing_date %}
    date('2022-01-01')
{% endset %}

with users_and_creations_by_day as (
    select
        n_creations,
        date_trunc(date_day, month) as date_month

    from {{ ref('metrics_reporting_users_and_creations_by_day') }}
    where date_day >= {{ testing_date }}
        and date_day < {{ get_first_day_current_month() }}
),

users_and_creations_by_month as (
    select
        n_creations,
        date_month

    from {{ ref('metrics_reporting_users_and_creations_by_month') }}
    where date_month >= {{ testing_date }}
),

final as (
    {{ compare_metric_consistency_between_models(
        ctes = [
            'users_and_creations_by_day',
            'users_and_creations_by_month'
            ],
        metric = 'n_creations',
        date_column = 'date_month'
    ) }}
)

select * from final
