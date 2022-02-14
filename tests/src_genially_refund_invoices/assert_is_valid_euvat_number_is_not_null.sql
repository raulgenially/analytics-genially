-- is_valid_euvat_number should not be null for refunds that
-- reference invoices created after var('invoice_tax_start_date')
-- It can be null, however, if the invoice was created before that date.
with refunds as (
    select * from {{ ref('src_genially_refund_invoices') }}
    where date(invoiced_at) > '{{ var('invoice_tax_start_date') }}'
),

invoices as (
    select * from {{ ref('src_genially_invoices') }}
),

final as (
    select
        refunds.*

    from refunds
    inner join invoices
        on refunds.reference_invoice_number_id = invoices.invoice_number_id
    where refunds.is_valid_euvat_number is null
        and invoices.invoiced_at > '{{ var('invoice_tax_start_date') }}'
)

select * from final
