-- The number of signups should match among the implicated models.
{% set testing_date %}
    '{{ var('snapshot_users_start_date') }}'
{% endset %}

with login_activity_active_users as (
    select
        n_signups,
        date_day

    from {{ ref('metrics_login_activity_active_users') }}
    where date_day >= {{ testing_date }}
),

users_and_creations_by_day as (
    select
        n_signups,
        date_day

    from {{ ref('metrics_reporting_users_and_creations_by_day') }}
    where date_day >= {{ testing_date }}
),

final as (
    {{ compare_metric_consistency_between_models(
        ctes = [
            'login_activity_active_users',
            'users_and_creations_by_day'
            ],
        metric = 'n_signups',
        date_column = 'date_day'
    ) }}
)

select * from final
