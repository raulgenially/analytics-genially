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
    if(billing.product like '%Team%', '5', cast(quantity as string)) as quantity,
    regexp_replace(billing.product, r'(.*)Team*.', r'\1 Master') as product,
    billing.recurrence,
    billing.plan,
    replace(cast(billing.subtotal as string), '.',',') as subtotal,
    replace(cast(billing.tax_amount as string), '.',',') as tax_amount,
    replace(cast(billing.amount as string), '.',',') as total,
    replace(cast(billing.original_amount as string), '.',',') as original_total,
    billing.currency as original_currency,
    {# billing.description, #}
    billing.payer_name as fiscal_name,
    billing.payer_cif as tax_id,
    billing.payer_address as fiscal_adress,
    billing.payer_country as fiscal_country,
    billing.payer_email,
    billing.tax_key
    billing.tax_rate
    billing.is_valid_euvat_number,
    if(billing.payment_platform in ('PayPal', 'Braintree'), 'PayPal '||upper(billing.currency), billing.payment_platform) as payment_method,
    billing.eu,
    billing.iva as IVA,
    '705' as canal,
    billing.user_id,
    billing.transaction_id,
    billing.subscription_id,
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
