with refund_invoices as (
    select * from {{ source('genially', 'refundinvoice') }}
),

final as (
    select
        _id as invoice_id,

        data,
        description,
        payeremail as payer_email,
        residencecountry as residence_country,
        -- force all totals to be negative
        abs(cast(total as float64)) * -1 as total,
        abs(cast(ifnull(totaleuro, total) as float64)) * -1 as total_euro,
        ifnull(currency, 'eur') as currency,
        invoiceid as invoice_number,
        {{ map_payment_platform('realtransactionid') }} as payment_platform,

        iduser as user_id,
        transactionid as subscription_id,
        realtransactionid as transaction_id,
        referenceinvoice as reference_invoice_number,

        dateinvoce as invoiced_at,

    from refund_invoices
    where __hevo__marked_deleted = false
)

select * from final
