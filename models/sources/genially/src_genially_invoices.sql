with invoices as (
    {{ create_base_invoice_model('invoice') }}
),

final as (
    select
        _id as invoice_id,

        description,
        payeremail as payer_email,
        payer_name,
        payer_cif,
        payer_address,
        payer_country,
        residencecountry as residence_country,
        total_float as total,
        total_euro_float as total_euro,
        cleaned_currency as currency,
        invoiceid as invoice_number,
        payment_platform,

        iduser as user_id,
        transactionid as subscription_id,
        realtransactionid as transaction_id,

        dateinvoice as invoiced_at,

    from invoices
)

select * from final
