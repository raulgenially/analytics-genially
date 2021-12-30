-- This macro is intended to be used in activity-related models, across various status (daily, weekly/7 days and monthly/28 days)
-- Calculate the number of active users (active users = signups + current users)
{% macro create_metrics_activity_status_model(activity, status) %}

with final as (
    select
        -- Dimensions
        date_day,
        case
            when {{ status }} = 'Current'
                then 'Current Users'
            when {{ status }} = 'New'
                then 'Signups'
            else {{ status }}
        end as {{ status }},
        {{ place_main_dimension_fields('activity') }},
        device,
        acquisition_channel,

        -- Metrics
        count(user_id) as n_users

    from {{ activity }}
    where {{ status }} in ('New', 'Current')
    {{ dbt_utils.group_by(n=12) }}

    union all

    select
        -- Dimensions
        date_day,
        'Total Active Users' as {{ status }},
        {{ place_main_dimension_fields('activity') }},
        device,
        acquisition_channel,

        -- Metrics
        count(user_id) as n_users

    from {{ activity }}
    where {{ status }} in ('New', 'Current')
    {{ dbt_utils.group_by(n=12) }}
)

select * from final

{% endmacro %}
