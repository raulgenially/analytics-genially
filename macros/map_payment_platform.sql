{% macro map_payment_platform(realtransactionid) %}
    case
        when {{ realtransactionid }} like 'ch_%'
            -- example: ch_1IssJSBn82mIxvX2icHUlNiT
            then 'Stripe'
        when {{ realtransactionid }} like '%-%-%-%-%'
            -- example: 4115d837-f87b-49dd-a1ab-8e6c8aa842ff
            or {{ realtransactionid }} like 'PP-D-%'
            -- example: PP-D-100420477
            or length({{ realtransactionid }}) = 17
            -- example: 1HA94865MS471004N
            then 'PayPal'
        when length({{ realtransactionid }}) = 8
            -- example: 1hsbxwj5
            then 'Braintree'
    end
{% endmacro %}
