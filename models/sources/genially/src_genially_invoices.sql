with invoices as (
    select * from {{ source('genially', 'invoice') }}
),

int_invoices as (
    select
        *,

        trim(json_extract_scalar(data, '$.Name')) as payer_name,
        trim(json_extract_scalar(data, '$.Cif')) as payer_cif,
        trim(json_extract_scalar(data, '$.Address')) as payer_address,
        trim(json_extract_scalar(data, '$.Country')) as payer_country,

    from invoices
),

final as (
    select
        _id as invoice_id,

        description,
        payeremail as payer_email,
        -- Some cifs/addresses are denoted with '-', ' ', '.' or '--'. Nullify them
        if(regexp_contains(payer_name, r'\w+'), payer_name, null) as payer_name,
        if(regexp_contains(payer_cif, r'\w+'), payer_cif, null) as payer_cif,
        if(regexp_contains(payer_address, r'\w+'), payer_address, null) as payer_address,
        payer_country,
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

    from int_invoices
    where __hevo__marked_deleted = false
)

select * from final
