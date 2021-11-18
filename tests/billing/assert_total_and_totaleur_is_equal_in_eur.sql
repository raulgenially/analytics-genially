-- Check if total = total_eur when currenty = 'eur'
-- This chek is also valid to

with invoices as (
   {{create_billing_model('src_genially_invoices')}}
),

refundinvoice as (
   {{create_billing_model('src_genially_refund_invoices')}}
),

billing as (
    select
        "Sale" as invoice_type,
        *
    from invoices
    union all
    select
        "Refund" as invoice_type,
        *
    from refundinvoice
),

final as (
    select
        invoice_id,
        invoice_number,
        currency,
        total,
        total_euro

    from billing
    where currency = 'eur'
    and total != total_euro
)

select * from final
