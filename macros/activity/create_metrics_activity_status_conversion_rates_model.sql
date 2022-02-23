-- This macro is intended to be used in activity-related models, across various status (daily, weekly/7 days and monthly/28 days)
-- Compute conversion rates between stages. For example: activation (new -> returning), retention (returning -> returning), etc.
{% macro create_metrics_activity_status_conversion_rates_model(activity, status) %}

with final as (
    select
       -- Dimensions
        date_day,
        {{ place_main_dimension_fields('activity') }},
        signup_device,
        signup_channel,

        -- Transition Type
        countif({{ status }} = 'Returning' and previous_{{ status }} = 'New') as n_activated_users,
        countif({{ status }} = 'Churned' and previous_{{ status }} = 'New') as n_inactivated_users,
        countif({{ status }} = 'Returning' and previous_{{ status }} = 'Returning') as n_retained_users,
        countif({{ status }} = 'Churned' and previous_{{ status }} = 'Returning') as n_churned_users,
        countif({{ status }} = 'Returning' and previous_{{ status }} = 'Churned') as n_resurrected_users,
        countif({{ status }} = 'Churned' and previous_{{ status }} = 'Churned') as n_hibernated_users,

        -- Totals
        countif(previous_{{ status }} = 'New') as n_previous_new_users,
        countif(previous_{{ status }} = 'Returning') as n_previous_returnings_users,
        countif(previous_{{ status }} = 'Churned') as n_previous_churned_users

    from {{ activity }}
    {{ dbt_utils.group_by(n=11) }}
)

select * from final

{% endmacro %}
