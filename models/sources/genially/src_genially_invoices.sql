with invoices as (
    select * from {{ source('genially', 'invoice') }}
),

final as (
    select
        _id as invoice_id,

        data,
        description,
        payeremail as payer_email,
        residencecountry as residence_country,
        cast(total as float64) as total,
        cast(ifnull(totaleuro, total) as float64) as total_euro,
        ifnull(currency, 'eur') as currency,
        invoiceid as invoice_number,
        {{ map_payment_platform('realtransactionid') }} as payment_platform,

        iduser as user_id,
        transactionid as subscription_id,
        realtransactionid as transaction_id,

        dateinvoice as invoiced_at,

    from invoices
    where __hevo__marked_deleted = false
)

select * from final
