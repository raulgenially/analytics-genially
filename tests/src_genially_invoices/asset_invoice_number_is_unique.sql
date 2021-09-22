-- Check that the invoice number is unique. We can't do this with a schema test
-- because we have some errors in 2018 and 2019. This test checks that it does not happen again.
with invoices as (
    select * from {{ ref('src_genially_invoices') }}
),

duplicated_invoice_numbers as (
    select
        invoice_number,
        count(*)

    from invoices
    where invoices.invoiced_at > timestamp('2020-01-01')
    group by invoice_number
    having count(*) > 1
)

select * from duplicated_invoice_numbers
