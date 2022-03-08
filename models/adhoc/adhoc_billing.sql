with billing as(
    select * from {{ ref('billing') }}
)

select
    if(billing.is_refund, 'Refund', 'Invoice') as type,
    if(billing.description like '%License%', 'Subscription', 'One Time') as mrr,
    format_date('%m', billing.invoiced_at) as month,
    if(billing.is_refund, 'RE'||billing.invoice_number_id, billing.invoice_number_id) as invoice_number_id,
    format_date('%d/%m/%Y', billing.invoiced_at) as invoiced_date,
    format_date('%d/%m/%Y', billing.period_end_at) as invoice_end_date,
    date_diff(billing.period_end_at, billing.period_start_at, day) as days,
    if(billing.product like '%Team%', '5', cast(quantity as string)) as quantity,
    regexp_replace(billing.product, r'(.*)Team*.', r'\1Master') as product,
    billing.recurrence,
    billing.plan,
    replace(cast(billing.total_euro_deducted as string), '.',',') as subtotal,
    replace(cast(billing.tax_amount as string), '.',',') as tax_amount,
    replace(cast(billing.total_euro as string), '.',',') as total,
    replace(cast(billing.total as string), '.',',') as original_total,
    billing.currency as original_currency,
    billing.payer_name as fiscal_name,
    billing.payer_cif as tax_id,
    billing.payer_address as fiscal_adress,
    billing.payer_country as fiscal_country,
    billing.payer_email,
    billing.tax_key,
    billing.tax_rate,
    billing.is_valid_euvat_number,
    if(billing.payment_platform in ('PayPal', 'Braintree'), 'PayPal '||upper(billing.currency), billing.payment_platform) as payment_method,
    case
        when tax_key = 'INTRA_21'
        and is_valid_euvat_number
            then 'Intracommunitary'
        when is_from_eu_country
            then 'Communitary'
        else 'Extracommunitary'
    end as eu,
    if(
        billing.is_from_eu_country
        and tax_key <> 'INTRA_21',
        'IVA',
        'No-IVA'
    ) as IVA,
    '705' as canal,
    billing.user_id,
    billing.transaction_id,
    billing.subscription_id,
    billing.role,
    billing.sector,
    billing.reference_invoice_number_id,
    format_date(
        '%d/%m/%Y',
        if(
            billing.is_refund,
            billing.originally_invoiced_at,
            null
        )
    ) as refund_original_invoice_date,
    if(
            tax_key in ('INTRA_21', 'IVA_ES_21', 'NOSUJ'),
            '01',
            '17'
    ) as SII_vat_regime,
    if(
        least(billing.payer_cif, billing.payer_address, billing.payer_name) is not null
        or total_euro_deducted > 400,
        if(billing.is_refund,
            null,
            'F1'),
        if(billing.is_refund,
            'R5',
            'F2')
    ) as SII_invoice_type

from billing
where date(billing.invoiced_at) >= current_date() - 30
