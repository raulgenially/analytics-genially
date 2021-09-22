-- Check that the payment_platform is not None on new invoices.
-- This will ensure we are aware when a new payment_platform is added.
with invoices as (
    select * from {{ ref('src_genially_invoices') }}
)

select
    invoice_id,
    invoice_number,
    subscription_id,
    transaction_id,
    invoiced_at
from invoices
where payment_platform is null
    and invoiced_at > timestamp('2020-08-15')
order by invoiced_at desc
