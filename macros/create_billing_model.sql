-- Based on macros/create_base_invoice_model.
-- The models src_genially_invoice and src_genially_refundinvoice are almost identical.
-- Therefore, the data transformations to be performed are pretty much the same.
-- Here we create a stg model where all the transformations common to both models are applied.
-- This macro is intended to be used in the stg models for both collections.
{% macro create_billing_model(table_source) %}

with invoices as (
    select * from {{ ref(table_source) }}
    where invoiced_at >= '{{ var('invoice_duration_start_date') }}'
),

final as (
    select
        invoice_id,
        description,
        payer_email,
        payer_name,
        payer_cif,
        payer_address,
        payer_country,
        total,
        total_euro,
        currency,
        cast(invoice_number as string) as invoice_number,
        payment_platform,
        user_id,
        subscription_id,
        transaction_id,
        invoiced_at,

        --Invoices description has the same structure: 1 x Annual Student License Genial.ly (31 OCT 2021 - 31 OCT 2022)
        --We can extract all the next fields below this with regexps
        parse_date('%d %b %Y', regexp_extract(description, r'\((.*?)\-')) as period_start_at,
	    parse_date('%d %b %Y', regexp_extract(description, r'\- (.*?)\)')) as period_end_at,
        regexp_extract(description, r'(.*?)\sx') as quantity,
        regexp_extract(description, r'x (.*?) License') as product,
        regexp_extract(description, r'x (.*?)\s') as recurrency,
        regexp_extract(description, r'\s(.*?) License', 5) as plan,

    from invoices
)

select * from final

{% endmacro %}
