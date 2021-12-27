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
    {{ get_duplicated_records('refunds', 'invoice_id') }}
),

-- Check that there are no duplicates among invoices
dup_invoices as (
    {{ get_duplicated_records('invoices', 'invoice_id') }}
),

final as (
    select * from dup_invoices
    union all
    select * from dup_refunds
)

select * from final
