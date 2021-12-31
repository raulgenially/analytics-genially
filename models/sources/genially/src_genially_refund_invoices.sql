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
        total_float as total,
        total_euro_float as total_euro,
        tax_rate,
        tax_key,
        cleaned_currency as currency,
        payment_platform,

        is_valid_euvat_number,

        iduser as user_id,
        transactionid as subscription_id,
        realtransactionid as transaction_id,
        invoiceid as invoice_number_id,
        referenceinvoice as reference_invoice_number_id,

        dateinvoce as invoiced_at,

    from invoices
)

select * from final
