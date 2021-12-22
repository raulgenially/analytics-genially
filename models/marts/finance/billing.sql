with invoices as (
    select * from {{ ref('stg_invoices') }}
    where invoiced_at >= '{{ var('invoice_duration_start_date') }}'
),

users as (
    select * from {{ ref('stg_users') }}
),

base_billing as (
    select
        *,
        if(is_refund, 'Refund', 'Invoice') as invoice_type,

        --Invoices description has the same structure: 1 x Annual Student License Genial.ly (31 OCT 2021 - 31 OCT 2022)
        --We can extract all the next fields below this with regexps
        -- TODO: remove safe.parse_date when the record causing
        -- problem (invoice_number=2021151505) is in a correct state
        safe.parse_date('%d %b %Y', regexp_extract(description, r'\((.*?)\-')) as period_start_at,
        safe.parse_date('%d %b %Y', regexp_extract(description, r'\- (.*?)\)')) as period_end_at,
        regexp_extract(description, r'(.*?)\sx') as quantity,
        regexp_extract(description, r'x (.*?) License') as product,
        regexp_extract(description, r'x (.*?)\s') as recurrence,
        regexp_extract(description, r'\s(.*?) License', 5) as plan,

        {{define_eu_countries('payer_country')}} as is_from_eu_country
    from invoices
),

int_billing as (
    select
        *,
        -- Apply tax to EU countries
        if(
            is_from_eu_country,
            total_euro / 1.21,
            total_euro
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
        billing.invoice_number,
        billing.reference_invoice_number,
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
        round(billing.total_euro_deducted, 4) as subtotal,
        round(billing.total_euro - billing.total_euro_deducted, 4) as tax_amount,
        billing.total_euro as amount,
        billing.total as original_amount,
        billing.currency,
        billing.eu,
        billing.iva,
        billing.description,
        billing.payer_email,
        billing.payer_cif,
        billing.payer_address,
        billing.payer_country,
        billing.payment_platform,
        billing.payer_name,
        users.role,
        users.sector,

        billing.user_id,
        billing.subscription_id,
        billing.transaction_id,

        billing.invoiced_at,
        billing.originally_invoiced_at,
        billing.period_start_at_sanitized as period_start_at,
        billing.period_end_at_sanitized as period_end_at,

    from int_billing as billing
    left join users
        on billing.user_id = users.user_id
)

select * from final
