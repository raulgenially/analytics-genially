--Invoices and Refund Invoices has tax fields to calculate taxes.
--Tax charges depends of the payer country and other fiscal rules.
--The tax possibilities are given by finance team by a list (seed_taxkey_taxrates.csv)

with taxes as (
    select * from {{ ref('seed_taxkey_taxrate') }}
),

invoices as (
    select * from {{ ref('stg_invoices') }}
),

final as (
    select
        invoices.tax_key,
        invoices.tax_rate,
        taxes.tax_key,
        taxes.tax_rate

    from invoices
    left join taxes
        on invoices.tax_key = taxes.tax_key
    where invoiced_at > '{{var('invoice_tax_start_date')}}'
    and invoices.tax_rate != taxes.tax_rate
)

select * from final
