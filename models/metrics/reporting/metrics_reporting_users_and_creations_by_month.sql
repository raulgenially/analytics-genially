{% set date_part = "month" %}

with reporting_model as (
    {{ create_metrics_reporting_users_and_creations_model(date_part) }}
),

final as (
    select
        *,
        -- lags n_signups
        {{ get_lag_dimension_metrics_reporting('n_signups', 1, 'date_month') }}
            as n_signups_previous_month,
        {{ get_lag_dimension_metrics_reporting('n_signups', 12, 'date_month') }}
            as n_signups_previous_year,
        -- lags n_creations
        {{ get_lag_dimension_metrics_reporting('n_creations', 1, 'date_month') }}
            as n_creations_previous_month,
        {{ get_lag_dimension_metrics_reporting('n_creations', 12, 'date_month') }}
            as n_creations_previous_year,
        -- lag n_new_creators
        {{ get_lag_dimension_metrics_reporting('n_new_creators', 1, 'date_month') }}
            as n_new_creators_previous_month,
        {{ get_lag_dimension_metrics_reporting('n_new_creators', 12, 'date_month') }}
            as n_new_creators_previous_year,
        -- lag n_new_creators_registered_same_month
        {{ get_lag_dimension_metrics_reporting('n_new_creators_registered_same_month', 1, 'date_month') }}
            as n_new_creators_registered_same_month_previous_month,
        {{ get_lag_dimension_metrics_reporting('n_new_creators_registered_same_month', 12, 'date_month') }}
            as n_new_creators_registered_same_month_previous_year,
        -- lag n_new_creators_previously_registered
        {{ get_lag_dimension_metrics_reporting('n_new_creators_previously_registered', 1, 'date_month') }}
            as n_new_creators_previously_registered_previous_month,
        {{ get_lag_dimension_metrics_reporting('n_new_creators_previously_registered', 12, 'date_month') }}
            as n_new_creators_previously_registered_previous_year,
        --lag n_active_users
        {{ get_lag_dimension_metrics_reporting('n_active_users', 1, 'date_month') }}
            as n_active_users_previous_month,
        {{ get_lag_dimension_metrics_reporting('n_active_users', 12, 'date_month') }}
            as n_active_users_previous_year,
        --lag n_returning_users
        {{ get_lag_dimension_metrics_reporting('n_returning_users', 1, 'date_month') }}
            as n_returning_users_previous_month,
        {{ get_lag_dimension_metrics_reporting('n_returning_users', 12, 'date_month') }}
            as n_returning_users_previous_year

    from reporting_model
)

select * from final
