with billing as(
    select * from {{ ref('billing') }}
)

select
    billing.invoice_type as type,
    if(description like '%License%', 'Subscription', 'One Time') as mrr,
    format_date('%m', billing.invoiced_at) as month,
    if(billing.invoice_type = 'Refund', 'RE'||billing.invoice_number, billing.invoice_number) as invoice_number,
    format_date('%d/%m/%Y', billing.invoiced_at) as invoiced_date,
    format_date('%d/%m/%Y', billing.period_end_at) as invoice_end_date,
    days,
    if(billing.product like '%Team%', '5', quantity) as quantity,
    regexp_replace(billing.product, r'(.*)Team*.', r'\1 Master') as product,
    billing.recurrence,
    billing.plan,
    replace(cast(billing.subtotal as string), '.',',') as subtotal,
    replace(cast(billing.tax_amount as string), '.',',') as tax_amount,
    replace(cast(billing.amount as string), '.',',') as amount,
    replace(cast(billing.original_amount as string), '.',',') as original_amount,
    billing.currency,
    billing.description,
    billing.payer_email,
    billing.payer_cif,
    billing.payer_address,
    billing.payer_country,
    if(billing.payment_platform in ('Paypal', 'Braintree'), 'Paypal '||billing.currency, billing.payment_platform) as payment_method,
    billing.eu,
    billing.IVA,
    '705' as canal,
    billing.transaction_id,
    billing.subscription_id,
    billing.user_id,
    billing.payer_name,
    billing.role,
    billing.sector,
    billing.reference_invoice_number,
    format_date(
        '%d/%m/%Y',
        if(
            billing.invoice_type = 'Refund',
            billing.originally_invoiced_at,
            null
        )
    ) as refund_original_invoice_date

from billing
where date(billing.invoiced_at) >= current_date() - 30
