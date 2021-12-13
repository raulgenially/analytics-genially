-- stg_invoices joins both invoices and refunds. Both invoices and refunds are enriched with
-- information from the analogous collection via invoice numbers which are not always unique.
-- Here we test that we've taken care of that so that we don't duplicate invoices/refunds due to joins.
with source as (
    select * from {{ ref('stg_invoices') }}
),

refunds as (
    select * from source
    where is_refund = true
),

invoices as (
    select * from source
    where is_refund = false
),

-- Check that there are no duplicates among refunds
dup_refunds as (
    select
        invoice_id as unique_field,
        count(invoice_id) as n_records

    from refunds
    group by invoice_id
    having n_records > 1
),

-- Check that there are no duplicates among invoices
dup_invoices as (
    select
        invoice_id as unique_field,
        count(invoice_id) as n_records

    from invoices
    group by invoice_id
    having n_records > 1
),

final as (
    select * from dup_invoices
    union all
    select * from dup_refunds
)

select * from final
