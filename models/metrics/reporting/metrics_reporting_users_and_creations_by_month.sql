{% set min_date_activity %}
    date('2022-01-01') -- we are collecting data since '2021-12-20', so first complete month is 01-2022.
{% endset %}

{% set min_date %}
    date('2020-01-01')
{% endset %}

with reference_table as (
    {{ get_combination_calendar_dimensions(min_date, "month") }}
),

login_activity as (
    select
        *,
        date_trunc(date_day, month) as date_month

    from {{ ref('metrics_login_activity') }}
    where status in ('New', 'Returning')
        and date_day >= {{ min_date_activity }}
),

users_and_creations_by_day as (
    select
        *,
        date_trunc(date_day, month) as date_month

    from {{ ref('metrics_reporting_users_and_creations_by_day') }}
),

metrics1 as (
    select
        -- Dimensions
        date(reference_table.date_month) as date_month,
        reference_table.plan,
        reference_table.subscription,
        reference_table.country,
        reference_table.country_name,
        reference_table.broad_sector,
        reference_table.broad_role,
        -- Metrics
        count(distinct login_activity.user_id) as n_active_users

    from reference_table
    left join login_activity
        on date(reference_table.date_month) = login_activity.date_month
            and reference_table.plan = login_activity.plan
            and reference_table.subscription = login_activity.subscription
            and reference_table.country = login_activity.country
            and reference_table.country_name = login_activity.country_name
            and reference_table.broad_sector = login_activity.broad_sector
            and reference_table.broad_role = login_activity.broad_role
    {{ dbt_utils.group_by(n=7) }}
),

creations as(
    select
        -- Dimensions
        date_month,
        plan,
        subscription,
        country,
        country_name,
        broad_sector,
        broad_role,
        -- Metrics
        sum(n_creations) as n_creations,
        sum(n_signups) as n_signups

    from users_and_creations_by_day
    where date_month >= {{ min_date }}
    {{ dbt_utils.group_by(n=7) }}
),

metrics2 as (
    select
        m1.date_month,
        m1.plan,
        m1.subscription,
        m1.country,
        m1.country_name,
        m1.broad_sector,
        m1.broad_role,
        m1.n_active_users,
        c.n_creations as n_creations,
        c.n_signups as n_signups

    from metrics1 as m1
    left join creations as c
        on m1.date_month = c.date_month
            and m1.plan = c.plan
            and m1.subscription = c.subscription
            and m1.country = c.country
            and m1.country_name = c.country_name
            and m1.broad_sector = c.broad_sector
            and m1.broad_role = c.broad_role
),

final as (
    select
        *,
        --lags n_active_users
        {{ get_lag_dimension_metrics_reporting('n_active_users', 1, 'date_month') }} as n_active_users_previous_month,
        -- lags n_signups
        {{ get_lag_dimension_metrics_reporting('n_signups', 1, 'date_month') }} as n_signups_previous_month,
        {{ get_lag_dimension_metrics_reporting('n_signups', 12, 'date_month') }} as n_signups_previous_year,
        -- lags n_creations
        {{ get_lag_dimension_metrics_reporting('n_creations', 1, 'date_month') }} as n_creations_previous_month,
        {{ get_lag_dimension_metrics_reporting('n_creations', 12, 'date_month') }} as n_creations_previous_year

    from metrics2
)

select * from final
