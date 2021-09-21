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
        case
            when realtransactionid like 'ch_%'
                -- example: ch_1IssJSBn82mIxvX2icHUlNiT
                then 'Stripe'
            when realtransactionid like '%-%-%-%-%'
                -- example: 4115d837-f87b-49dd-a1ab-8e6c8aa842ff
                or length(realtransactionid) = 17
                -- example: 1HA94865MS471004N
                then 'Paypal'
            when length(realtransactionid) = 8
                -- example: 1hsbxwj5
                then 'Braintree'
        end as payment_platform,

        iduser as user_id,
        transactionid as subscription_id,
        realtransactionid as transaction_id,

        dateinvoice as invoiced_at,

    from invoices
    where __hevo__marked_deleted = false
)

select * from final
