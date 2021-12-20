{% macro map_license_status(license_status, canceled_code) %}
    case
        when {{ license_status }} = 'Pending'
            -- License pending payment
            then 'Pending'
        when {{ license_status }} = 'Finished'
            -- License paid
            then
                case
                    when {{ canceled_code }} is null
                        -- License paid and not canceled
                        then 'Active'
                    -- License paid and canceled before it has reached EOF
                    else 'Churn'
                end
        when {{ license_status }} = 'Canceled'
            -- License has reached EOF
            then
                case
                    when {{ canceled_code }} is null
                        -- License has reached EOF organically
                        then 'Finished'
                    -- License has reached EOF because the
                    -- user canceled the subscription
                    else 'Canceled'
                end
    end
{% endmacro %}
