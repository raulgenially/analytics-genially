-- This macro is intended to be used in activity-related models, across various status (daily, weekly/7 days and monthly/28 days)
{% macro create_metrics_activity_status_change_rates_model(activity, status) %}

with denominator as (
    select
        -- Dimensions
        date_day,
        previous_{{ status }},

        -- Metrics
        count(user_id) as n_users

    from {{ activity }}
    where previous_{{ status }} is not null
    group by 1, 2
),

numerator as (
    select
        -- Dimensions
        date_day,
        previous_{{ status }},
        {{ status }},

        -- Metrics
        count(user_id) as n_users

    from {{ activity }}
    where previous_{{ status }} is not null
    {{ dbt_utils.group_by(n=3) }}
),

final as (
    select
        -- Dimensions
        denominator.date_day,
        denominator.previous_{{ status }},
        numerator.{{ status }},
        case
            when denominator.previous_{{ status }} = 'New' and numerator.{{ status }} = 'Current'
                then 'Activation'
            when denominator.previous_{{ status }} = 'New' and numerator.{{ status }} = 'Churned'
                then 'Churn'
            when denominator.previous_{{ status }} = 'Current' and numerator.{{ status }} = 'Current'
                then 'Retention'
            when denominator.previous_{{ status }} = 'Current' and numerator.{{ status }} = 'Churned'
                then 'Churn'
            when denominator.previous_{{ status }} = 'Churned' and numerator.{{ status }} = 'Current'
                then 'Resurrection'
            when denominator.previous_{{ status }} = 'Churned' and numerator.{{ status }} = 'Churned'
                then 'Churn'
        end as transition_type,

        -- Metrics
        denominator.n_users as n_total_users_previous_{{ status }},
        numerator.n_users as n_passing_users_{{ status }}

    from denominator
    left join numerator
        on denominator.date_day = numerator.date_day
            and denominator.previous_{{ status }} = numerator.previous_{{ status }}
    order by 1 asc
)

select * from final

{% endmacro %}