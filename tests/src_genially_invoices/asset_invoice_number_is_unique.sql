-- Check that the invoice number is unique. We can't do this with a schema test
-- because we have some errors in 2018 and 2019. This test checks that it does not happen again.
with invoices as (
    select * from {{ ref('src_genially_invoices') }}
)

select
    a.*
from invoices as a
join (
    select invoice_number, count(*)
    from invoices
    group by invoice_number
    having count(*) > 1
) as b
  on a.invoice_number = b.invoice_number
where a.invoiced_at > timestamp('2020-01-01')
order by a.invoiced_at desc
