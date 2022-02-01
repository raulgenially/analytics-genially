-- The collections genially.invoice and genially.refundinvoice are almost identical.
-- Therefore, the data transformations to be performed are pretty much the same.
-- Here we create a base model where all the transformations common to both models are applied.
-- This macro is intended to be used in the source models for both collections.
{% macro create_base_invoice_model(table_source, invoiced_at_field) %}

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
        cast(trim(json_extract_scalar(data, '$.IsValidEuVatNumber')) as bool) as is_valid_euvat_number,
        cast(trim(json_extract_scalar(data, '$.TaxRate')) as int64) as tax_rate,
        trim(json_extract_scalar(data, '$.TaxKey')) as tax_key,

        -- Invoices created after 2020-11-01 have their description with the
        -- same structure: 1 x Annual Student License Genial.ly (31 OCT 2021 - 31 OCT 2022)
        -- We can extract all the next fields below this with regexps
        safe.parse_date('%d %b %Y', regexp_extract(description, r'\((.*?) \- (?:.*?)\)')) as period_start_at_raw,
        safe.parse_date('%d %b %Y', regexp_extract(description, r'\((?:.*?) \- (.*?)\)')) as period_end_at_raw,
        regexp_extract(description, r'^(\d+) x ') as quantity_raw,
        regexp_extract(description, r'^\d+ x (.*?) License') as product,
        regexp_extract(description, r'^\d+ x (Annual|Monthly) ') as recurrence_raw,
        regexp_extract(description, r'^\d+ x (?:Annual|Monthly) (.*?) License') as plan,

        -- invoice has dateinvoice and refundinvoice has dateinvoce as a invoiced_at field
        -- Here we rename the field so that it is the same in both cases
        {{ invoiced_at_field }} as invoiced_at

    from invoices
),

final as (
    select
        *,

        -- Some cifs/addresses are denoted with '-', ' ', '.' or '--'. Nullify them
        if(regexp_contains(data_payer_name, r'\w+'), data_payer_name, null) as payer_name,
        if(regexp_contains(data_payer_cif, r'\w+'), data_payer_cif, null) as payer_cif,
        if(regexp_contains(data_payer_address, r'\w+'), data_payer_address, null) as payer_address,
        {{ clean_payer_country('data_payer_country') }} as payer_country,

        -- description extracted fields sanitizing
        ifnull(cast(quantity_raw as int64), 1) as quantity,
        ifnull(recurrence_raw, 'Annual') as recurrence,
        ifnull(period_start_at_raw, date(invoiced_at)) as period_start_at,
        ifnull(
            period_end_at_raw,
            if(
                recurrence_raw = 'Monthly',
                date(invoiced_at) + 30,
                date(invoiced_at) + 360
            )
        ) as period_end_at,

    from int_invoices
)

select * from final

{% endmacro %}
