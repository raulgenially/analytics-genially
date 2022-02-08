-- This macro is intended to be used in activity-related models, across various status (daily, weekly/7 days and monthly/28 days)
-- Compute conversion rates between stages. For example: activation (new -> returning), retention (returning -> returning), etc.
{% macro create_metrics_activity_status_conversion_rates_model(activity, status) %}

with denominator as (
    select
        -- Dimensions
        date_day,
        previous_{{ status }} as previous_status,
        {{ place_main_dimension_fields('activity') }},
        signup_device,
        signup_channel,

        -- Metrics
        count(user_id) as n_users

    from {{ activity }}
    where previous_{{ status }} is not null
    {{ dbt_utils.group_by(n=12) }}
),

numerator as (
    select
        -- Dimensions
        date_day,
        previous_{{ status }} as previous_status,
        {{ status }} as status,
        {{ place_main_dimension_fields('activity') }},
        signup_device,
        signup_channel,

        -- Metrics
        count(user_id) as n_users

    from {{ activity }}
    where previous_{{ status }} is not null
    {{ dbt_utils.group_by(n=13) }}
),

final as (
    select
        -- Dimensions
        denominator.date_day,
        denominator.previous_status,
        numerator.status,
        {{ place_main_dimension_fields('denominator') }},
        denominator.signup_device,
        denominator.signup_channel,
        case
            when denominator.previous_status = 'New' and numerator.status = 'Returning'
                then 'Activation'
            when denominator.previous_status = 'New' and numerator.status = 'Churned'
                then 'Inactivation'
            when denominator.previous_status = 'Returning' and numerator.status = 'Returning'
                then 'Retention'
            when denominator.previous_status = 'Returning' and numerator.status = 'Churned'
                then 'Churn'
            when denominator.previous_status = 'Churned' and numerator.status = 'Returning'
                then 'Resurrection'
            when denominator.previous_status = 'Churned' and numerator.status = 'Churned'
                then 'Hibernation'
        end as transition_type,

        -- Metrics
        denominator.n_users as n_initial_users,
        numerator.n_users as n_passing_users

    from denominator
    left join numerator
        on denominator.date_day = numerator.date_day
            and denominator.previous_status = numerator.previous_status
            and denominator.plan = numerator.plan
            and denominator.subscription = numerator.subscription
            and denominator.sector = numerator.sector
            and denominator.broad_sector = numerator.broad_sector
            and denominator.role = numerator.role
            and denominator.broad_role = numerator.broad_role
            and denominator.country = numerator.country
            and denominator.country_name = numerator.country_name
            and denominator.signup_device = numerator.signup_device
            and denominator.signup_channel = numerator.signup_channel
)

select * from final

{% endmacro %}
