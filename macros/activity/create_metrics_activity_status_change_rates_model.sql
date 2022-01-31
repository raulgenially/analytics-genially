-- This macro is intended to be used in activity-related models, across various status (daily, weekly/7 days and monthly/28 days)
-- Compute change rates between stages. For example: activation (new -> current), retention (current -> current), etc.
{% macro create_metrics_activity_status_change_rates_model(activity, status) %}

with denominator as (
    select
        -- Dimensions
        date_day,
        previous_{{ status }},
        {{ place_main_dimension_fields('activity') }},
        device,
        channel,

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
        previous_{{ status }},
        {{ status }},
        {{ place_main_dimension_fields('activity') }},
        device,
        channel,

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
        denominator.previous_{{ status }},
        numerator.{{ status }},
        {{ place_main_dimension_fields('denominator') }},
        denominator.device,
        denominator.channel,
        case
            when denominator.previous_{{ status }} = 'New' and numerator.{{ status }} = 'Current'
                then 'Activation'
            when denominator.previous_{{ status }} = 'Current' and numerator.{{ status }} = 'Current'
                then 'Retention'
            when denominator.previous_{{ status }} = 'Churned' and numerator.{{ status }} = 'Current'
                then 'Resurrection'
        end as transition_type,

        -- Metrics
        denominator.n_users as n_initial_users,
        numerator.n_users as n_passing_users

    from denominator
    left join numerator
        on denominator.date_day = numerator.date_day
            and denominator.previous_{{ status }} = numerator.previous_{{ status }}
            and denominator.plan = numerator.plan
            and denominator.subscription = numerator.subscription
            and denominator.sector = numerator.sector
            and denominator.broad_sector = numerator.broad_sector
            and denominator.role = numerator.role
            and denominator.broad_role = numerator.broad_role
            and denominator.country = numerator.country
            and denominator.country_name = numerator.country_name
            and denominator.device = numerator.device
            and denominator.channel = numerator.channel
)

select * from final

{% endmacro %}
