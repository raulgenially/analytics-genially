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
        "Sale" as invoice_type,
        *
    from invoices
    union all
    select
        "Refund" as invoice_type,
        *
    from refundinvoice
),

final as (
    select
        invoice_type,
        if(invoice_type = 'Refund', 'RE'||invoice_number, invoice_number) as invoice_number,
        format_date('%d/%m/%Y', invoiced_at) as invoiced_at,
        format_date('%d/%m/%Y', (ifnull(date(finished_at),  period_end_at))) as period_end_at,
        date_diff(period_end_at, period_start_at, day) as days,
        if(product like '%Team%', '5', quantity) as quantity,
        regexp_replace(product, r'(.*)Team*.', r'\1 Master') as product,
        recurrency,
        billing.plan,
        round(if({{define_eu_countries('payer_country')}}
            and currency = 'eur', total/1.21, total_euro), 4) as subtotal,
        round(total_euro - if({{define_eu_countries('payer_country')}}
            and currency = 'eur', total/1.21, total_euro), 4) as tax_amount,
        total_euro as amount,
        total as original_amount,
        currency,
        description,
        payer_email,
        payer_cif,
        payer_address,
        payer_country,
        payment_platform,
        if({{define_eu_countries('payer_country')}},'Communitary','Extracommunitary') as eu,
        if({{define_eu_countries('payer_country')}} and currency = 'eur','IVA','No-IVA') as IVA,
        '705' as canal,
        billing.transaction_id,
        billing.subscription_id,
        billing.user_id,
        payer_name,
        role,
        sector

    from billing
    left join licenses
        on billing.subscription_id = licenses.subscription_id
    left join users
        on billing.user_id = users.user_id
)

select * from final
