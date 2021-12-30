{% macro map_license_status(license_status, canceled_code) %}
    case
        when {{ license_status }} = 'Pending'
            -- License pending payment
            then 'Pending'
        when {{ license_status }} = 'Finished'
            -- License paid
            then if(
                {{ canceled_code }} is null,
                -- License paid and not canceled
                'Active',
                -- License paid and canceled: the license is currently active but it will a proactive churn in the upcoming billing period
                'Canceled'
            )
        when {{ license_status }} = 'Canceled'
            -- License has reached EOL
            then if(
                {{ canceled_code }} is null,
                -- License has reached EOL because, for whatever reason,
                -- the customer didn't pay the subscription without canceling previously
                'Passive Churn',
                -- License has reached EOL because the
                -- user canceled the subscription at some point before the next billing period
                'Proactive Churn'
            )
    end
{% endmacro %}
