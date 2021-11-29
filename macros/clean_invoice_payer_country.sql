{% macro clean_payer_country(payer_country) %}
    case
        when lower({{ payer_country }}) in ('spain', 'españa')
            then 'ES'
        when lower({{ payer_country }}) in ('mexico', 'méxico.', 'méxico')
            then 'MX'
        when lower({{ payer_country }}) = 'italy'
            then 'IT'
        when {{payer_country }} = 'C2' -- China code in some platforms
            then 'CN'
        when lower({{ payer_country }}) = 'chile'
            then 'CL'
        when lower({{ payer_country }}) = 'colombia'
            then 'CO'
        when lower({{ payer_country }}) = 'france'
            then 'FR'
        when lower({{ payer_country }}) = 'belgium'
            then 'BE'
        when lower({{ payer_country }}) = 'malaysia'
            then 'MY'
        when lower({{ payer_country }}) = 'portugal'
            then 'PT'
        when lower({{ payer_country }}) = 'greece'
            then 'GR'
        when lower({{ payer_country }}) = 'guatemala'
            then 'GT'
        else {{ payer_country }}
    end
{% endmacro %}
