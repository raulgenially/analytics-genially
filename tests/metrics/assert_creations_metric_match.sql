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

creations_activity as (
    select
        sum(n_creations) as n_creations

    from login_activity_active_users
    where date_day >= {{ testing_date }}
),

creations_monthly_projections as (
    select
        sum(n_creations) as n_creations

    from monthly_projections
    where date_day >= {{ testing_date }}
),

final as (
    select
        creations_activity.n_creations as activity_creations,
        creations_monthly_projections.n_creations as monthly_projections_creations

    from creations_activity
    cross join creations_monthly_projections
    where creations_activity.n_creations != creations_monthly_projections.n_creations
)

select * from final
