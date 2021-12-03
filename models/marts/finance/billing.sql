with invoices as (
    select * from {{ ref('stg_invoices') }}
    where invoiced_at >= '{{ var('invoice_duration_start_date') }}'
),

users as (
    select * from {{ ref('stg_users') }}
),

billing_eu as (
    select
        *,
        {{define_eu_countries('payer_country')}} as is_from_eu_country
    from invoices
),

billing as (
    select
        *,
        if(is_refund, 'Refund', 'Invoice') as invoice_type,

        --Invoices description has the same structure: 1 x Annual Student License Genial.ly (31 OCT 2021 - 31 OCT 2022)
        --We can extract all the next fields below this with regexps
        parse_date('%d %b %Y', regexp_extract(description, r'\((.*?)\-')) as period_start_at,
        parse_date('%d %b %Y', regexp_extract(description, r'\- (.*?)\)')) as period_end_at,
        regexp_extract(description, r'(.*?)\sx') as quantity,
        regexp_extract(description, r'x (.*?) License') as product,
        regexp_extract(description, r'x (.*?)\s') as recurrence,
        regexp_extract(description, r'\s(.*?) License', 5) as plan,

        -- Apply tax to EU countries
        if(
            is_from_eu_country,
            total_euro / 1.21,
            total_euro
        ) as total_euro_taxed,
    from billing_eu
),

final as (
    select
        billing.transaction_id,
        billing.subscription_id,
        billing.user_id,

        billing.invoice_type,
        billing.invoice_number,
        billing.reference_invoice_number,

        date_diff(billing.period_end_at, billing.period_start_at, day) as days,
        billing.quantity,
        billing.product,
        billing.recurrence,
        billing.plan,
        round(billing.total_euro_taxed, 4) as subtotal,
        round(billing.total_euro - billing.total_euro_taxed, 4) as tax_amount,
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

        if(
            billing.is_from_eu_country,
            'Communitary',
            'Extracommunitary'
        ) as eu,
        if(
            billing.is_from_eu_country,
            'IVA',
            'No-IVA'
        ) as IVA,

        billing.invoiced_at,
        billing.originally_invoiced_at,
        ifnull(
            billing.period_end_at,
            if(
                billing.recurrence like 'Annual',
                date(billing.invoiced_at) + 360,
                date(billing.invoiced_at) + 30
            )
        ) as period_end_at

    from billing
    left join users
        on billing.user_id = users.user_id
)

select * from final
