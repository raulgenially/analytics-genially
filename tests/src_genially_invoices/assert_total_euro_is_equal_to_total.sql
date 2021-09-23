-- Check that total_euro is equal to total when the currency is eur.
with invoices as (
    select * from {{ ref('src_genially_invoices') }}
)

select
    *
from invoices
where total_euro != total
    and currency = 'eur'
