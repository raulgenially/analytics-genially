-- This marts is for billing team purposes.
-- It is not following our styling and ordering guide.
with invoices as (
   {{create_billing_model('src_genially_invoices')}}
),

refundinvoice as (
   {{create_billing_model('src_genially_refund_invoices')}}
),

licenses as (
    select * from {{ ref('src_genially_licenses') }}
),

users as (
    select * from {{ ref('stg_users') }}
),

billing as (
    select
        'Sale' as invoice_type,
        *
    from invoices
    union all
    select
        'Refund' as invoice_type,
        *
    from refundinvoice
),

final as (
    select
        billing.transaction_id,
        billing.subscription_id,
        billing.user_id,

        billing.invoice_type,
        billing.invoice_number,

        date_diff(billing.period_end_at, billing.period_start_at, day) as days,
        billing.quantity,
        billing.product,
        billing.recurrence,
        billing.plan,
        round(if({{define_eu_countries('payer_country')}}
            and billing.currency = 'eur', billing.total_euro/1.21, billing.total_euro), 4) as subtotal,
        round(billing.total_euro - if({{define_eu_countries('payer_country')}}
            and billing.currency = 'eur', billing.total_euro/1.21, billing.total_euro), 4) as tax_amount,
        billing.total_euro as amount,
        billing.total as original_amount,
        billing.currency,
        billing.description,
        billing.payer_email,
        billing.payer_cif,
        billing.payer_address,
        billing.payer_country,
        billing.payment_platform,
        billing.payer_name,
        users.role,
        users.sector,

        if({{define_eu_countries('payer_country')}},'Communitary','Extracommunitary') as eu,
        if({{define_eu_countries('payer_country')}} and billing.currency = 'eur','IVA','No-IVA') as IVA,

        billing.invoiced_at,
        ifnull(date(licenses.finished_at),  billing.period_end_at) as period_end_at

    from billing
    left join licenses
        on billing.subscription_id = licenses.subscription_id
    left join users
        on billing.user_id = users.user_id
)

select * from final
