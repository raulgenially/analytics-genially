{% set week_days = 7 %}
{% set month_days = 28 %}
{% set year_days = 364 %}

{% set min_date %}
    date('2020-01-01')
{% endset %}

{% set date_part = "day" %}

with reporting_model as (
    {{ create_metrics_reporting_users_and_creations_model(min_date, date_part) }}
),

final as (
    select
        *,
        -- lags n_signups
        {{ get_lag_dimension_metrics_reporting('n_signups', week_days, 'date_day') }}
            as n_signups_previous_7d,
        {{ get_lag_dimension_metrics_reporting('n_signups', month_days, 'date_day') }}
            as n_signups_previous_28d,
        {{ get_lag_dimension_metrics_reporting('n_signups', year_days, 'date_day') }}
            as n_signups_previous_364d,
        -- lags n_creations
        {{ get_lag_dimension_metrics_reporting('n_creations', week_days, 'date_day') }}
            as n_creations_previous_7d,
        {{ get_lag_dimension_metrics_reporting('n_creations', month_days, 'date_day') }}
            as n_creations_previous_28d,
        {{ get_lag_dimension_metrics_reporting('n_creations', year_days, 'date_day') }}
            as n_creations_previous_364d,
        -- lag n_new_creators
        {{ get_lag_dimension_metrics_reporting('n_new_creators', week_days, 'date_day') }}
            as n_new_creators_previous_7d,
        {{ get_lag_dimension_metrics_reporting('n_new_creators', month_days, 'date_day') }}
            as n_new_creators_previous_28d,
        {{ get_lag_dimension_metrics_reporting('n_new_creators', year_days, 'date_day') }}
            as n_new_creators_previous_364d,
        -- lag n_new_creators_registered_same_day
        {{ get_lag_dimension_metrics_reporting('n_new_creators_registered_same_day', week_days, 'date_day') }}
            as n_new_creators_registered_same_day_previous_7d,
        {{ get_lag_dimension_metrics_reporting('n_new_creators_registered_same_day', month_days, 'date_day') }}
            as n_new_creators_registered_same_day_previous_28d,
        {{ get_lag_dimension_metrics_reporting('n_new_creators_registered_same_day', year_days, 'date_day') }}
            as n_new_creators_registered_same_day_previous_364d,
        -- lag n_new_creators_previously_registered
        {{ get_lag_dimension_metrics_reporting('n_new_creators_previously_registered', week_days, 'date_day') }}
            as n_new_creators_previously_registered_previous_7d,
        {{ get_lag_dimension_metrics_reporting('n_new_creators_previously_registered', month_days, 'date_day') }}
            as n_new_creators_previously_registered_previous_28d,
        {{ get_lag_dimension_metrics_reporting('n_new_creators_previously_registered', year_days, 'date_day') }}
            as n_new_creators_previously_registered_previous_364d,
        --lag n_active_users
        {{ get_lag_dimension_metrics_reporting('n_active_users', week_days, 'date_day') }}
            as n_active_users_previous_7d,
        {{ get_lag_dimension_metrics_reporting('n_active_users', month_days, 'date_day') }}
            as n_active_users_previous_28d,
        {{ get_lag_dimension_metrics_reporting('n_active_users', year_days, 'date_day') }}
            as n_active_users_previous_364d,
        --lag n_returning_users
        {{ get_lag_dimension_metrics_reporting('n_returning_users', week_days, 'date_day') }}
            as n_returning_users_previous_7d,
        {{ get_lag_dimension_metrics_reporting('n_returning_users', month_days, 'date_day') }}
            as n_returning_users_previous_28d,
        {{ get_lag_dimension_metrics_reporting('n_returning_users', year_days, 'date_day') }}
            as n_returning_users_previous_364d

    from reporting_model
)

select * from final
