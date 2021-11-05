with invoices as (
    {{ create_base_invoice_model('refundinvoice') }}
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
        -- force all totals to be negative
        abs(total_float) * -1 as total,
        abs(total_euro_float) * -1 as total_euro,
        cleaned_currency as currency,
        invoiceid as invoice_number,
        payment_platform,

        iduser as user_id,
        transactionid as subscription_id,
        realtransactionid as transaction_id,
        referenceinvoice as reference_invoice_number,

        dateinvoce as invoiced_at,

    from invoices
)

select * from final
