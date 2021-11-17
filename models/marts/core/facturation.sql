--This marts is for facturation team purposes.
--It is not following our styling and ordering guide.
with invoices as (
   {{create_facturation_model('src_genially_invoices')}}
),

refundinvoice as (
   {{create_facturation_model('src_genially_refund_invoices')}}
),

licenses as (
    select * from {{ ref('src_genially_licenses') }}
),

users as (
    select * from {{ ref('stg_users') }}
),

facturation as (
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
        facturation.plan,
        round(if({{define_eu_countries('payer_country')}}
            and total = total_euro, total/1.21, ifnull(total_euro,total)), 4) as subtotal,
        round(if({{define_eu_countries('payer_country')}}
            and total = total_euro, total/1.21, ifnull(total_euro,total)) - ifnull(total_euro,total), 4) as tax_amount,
        ifnull(total_euro, total) as amount,
        total as original_amount,
        currency,
        description,
        payer_email,
        payer_cif,
        payer_address,
        payer_country,
        payment_platform,
        if({{define_eu_countries('payer_country')}},'Communitary','Extracommunitary') as eu,
        if({{define_eu_countries('payer_country')}} and total = total_euro,'IVA','No-IVA') as IVA,
        '705' as canal,
        facturation.transaction_id,
        facturation.subscription_id,
        facturation.user_id,
        payer_name,
        role,
        sector

    from facturation
    left join licenses on facturation.subscription_id = licenses.subscription_id
    left join users on facturation.user_id = users.user_id
)

select * from final
