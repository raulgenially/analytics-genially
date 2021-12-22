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
                -- License paid and canceled before it has reached EOL
                'Canceled'
            )
        when {{ license_status }} = 'Canceled'
            -- License has reached EOL
            then if(
                {{ canceled_code }} is null,
                -- License has reached EOL organically
                'Finished',
                -- License has reached EOL because the
                -- user canceled the subscription
                'Churn'
            )
    end
{% endmacro %}
