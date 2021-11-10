-- The collections genially.invoice and genially.refundinvoice are almost identical.
-- Therefore, the data transformations to be performed are pretty much the same.
-- Here we create a base model where all the transformations common to both models are applied.
-- This macro is intended to be used in the source models for both collections.
{% macro create_base_invoice_model(table_source) %}

with invoices as (
    select * from {{ source('genially', table_source) }}
    where __hevo__marked_deleted = false
),

int_invoices as (
    select
        *,

        cast(total as float64) as total_float,
        cast(ifnull(totaleuro, total) as float64) as total_euro_float,
        ifnull(currency, 'eur') as cleaned_currency,
        {{ map_payment_platform('realtransactionid') }} as payment_platform,

        trim(json_extract_scalar(data, '$.Name')) as data_payer_name,
        trim(json_extract_scalar(data, '$.Cif')) as data_payer_cif,
        trim(json_extract_scalar(data, '$.Address')) as data_payer_address,
        trim(json_extract_scalar(data, '$.Country')) as data_payer_country,

    from invoices
),

final as (
    select
        *,

        -- Some cifs/addresses are denoted with '-', ' ', '.' or '--'. Nullify them
        if(regexp_contains(data_payer_name, r'\w+'), data_payer_name, null) as payer_name,
        if(regexp_contains(data_payer_cif, r'\w+'), data_payer_cif, null) as payer_cif,
        if(regexp_contains(data_payer_address, r'\w+'), data_payer_address, null) as payer_address,
        data_payer_country as payer_country,

    from int_invoices
)

select * from final

{% endmacro %}
