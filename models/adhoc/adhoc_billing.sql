with billing as(
    select * from {{ ref('billing') }}
)

select
    invoice_type,
        if(invoice_type = 'Refund', 'RE'||invoice_number, invoice_number) as invoice_number,
        format_date('%d/%m/%Y', invoiced_at) as invoiced_at,
        format_date('%d/%m/%Y', period_end_at) as period_end_at,
        days,
        if(product like '%Team%', '5', quantity) as quantity,
        regexp_replace(product, r'(.*)Team*.', r'\1 Master') as product,
        recurrence,
        plan,
        subtotal,
        tax_amount,
        amount,
        original_amount,
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
        transaction_id,
        subscription_id,
        user_id,
        payer_name,
        role,
        sector
from billing
where invoiced_at >= current_date() - 7
