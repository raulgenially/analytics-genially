with invoices as (
    select * from {{ ref('stg_invoices') }}
    where invoiced_at >= '{{ var('invoice_duration_start_date') }}'
),

users as (
    select * from {{ ref('stg_users') }}
),

taxkey_taxrate as (
    select * from {{ ref('seed_taxkey_taxrate') }}
),

eu_countries as (
    select
        substr(tax_key, 5, 2) as eu_country

    from taxkey_taxrate
    where tax_rate > 0
),

base_billing as (
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

        if(eu_countries.eu_country is not null, true, false) as is_from_eu_country

    from invoices
    left join eu_countries
        on invoices.payer_country = eu_countries.eu_country
),

int_billing as (
    select
        *,
        -- Apply taxes
        if(
            tax_rate > 0,
            total_euro / (1 + tax_rate/100),
            --Old logic to maintain retro-compatibility
            if(
                is_from_eu_country,
                total_euro / 1.21,
                total_euro)
        ) as total_euro_deducted,
        if(
            is_from_eu_country,
            'Communitary',
            'Extracommunitary'
        ) as eu,
        if(
            is_from_eu_country,
            'IVA',
            'No-IVA'
        ) as iva,

        ifnull(
            period_start_at,
            date(invoiced_at)
        ) as period_start_at_sanitized,

        ifnull(
            period_end_at,
            if(
                recurrence like '%Month%',
                date(invoiced_at) + 30,
                date(invoiced_at) + 360
            )
        ) as period_end_at_sanitized

    from base_billing
),

final as (
    select
        billing.id,

        billing.invoice_type,
        billing.description,
        billing.payer_email,
        billing.payer_name,
        billing.payer_cif,
        billing.payer_address,
        billing.payer_country,
        billing.total as original_amount,
        billing.total_euro as amount_euro,
        billing.tax_rate,
        billing.tax_key,
        billing.currency,
        round(billing.total_euro_deducted, 4) as subtotal,
        round(billing.total_euro - billing.total_euro_deducted, 4) as tax_amount,
        billing.eu,
        billing.iva,
        billing.payment_platform,
        date_diff(
            billing.period_end_at_sanitized,
            billing.period_start_at_sanitized,
            day
        ) as days,
        ifnull(
            cast(billing.quantity as int64),
            1
        ) as quantity,
        billing.product,
        billing.recurrence,
        billing.plan,
        users.role,
        users.sector,

        billing.is_valid_euvat_number,

        billing.user_id,
        billing.subscription_id,
        billing.transaction_id,
        billing.invoice_number_id,
        billing.reference_invoice_number_id,

        billing.invoiced_at,
        billing.originally_invoiced_at,
        billing.period_start_at_sanitized as period_start_at,
        billing.period_end_at_sanitized as period_end_at,

    from int_billing as billing
    left join users
        on billing.user_id = users.user_id
)

select * from final
