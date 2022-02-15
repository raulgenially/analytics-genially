with invoices as (
    select * from {{ ref('stg_invoices') }}
),

users as (
    select * from {{ ref('stg_users') }}
),

taxkey_taxrate as (
    select * from {{ ref('seed_taxkey_taxrate') }}
),

eu_countries as (
    select
        -- Example: OSS_HU_27 -> HU
        substr(tax_key, 5, 2) as eu_country

    from taxkey_taxrate
    where tax_rate > 0
),

base_billing as (
    select
        *,
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
            total_euro / (1 + tax_rate / 100),
            --Old logic to maintain retro-compatibility
            if(
                is_from_eu_country
                and tax_key <> 'INTRA_21',
                total_euro / 1.21,
                total_euro
            )
        ) as total_euro_deducted,

    from base_billing
),

final as (
    select
        billing.id,
        billing.invoice_id,

        billing.description,
        billing.quantity,
        billing.product,
        billing.recurrence,
        billing.plan,
        billing.payer_email,
        billing.payer_name,
        billing.payer_cif,
        billing.payer_address,
        billing.payer_country,
        billing.total,
        billing.currency,
        billing.total_euro,
        billing.tax_rate,
        billing.tax_key,
        round(billing.total_euro_deducted, 4) as total_euro_deducted,
        round(billing.total_euro - billing.total_euro_deducted, 4) as tax_amount,
        billing.payment_platform,
        users.role,
        users.sector,

        billing.is_refund,
        billing.is_valid_euvat_number,
        billing.is_from_eu_country,

        billing.user_id,
        billing.subscription_id,
        billing.transaction_id,
        billing.invoice_number_id,
        billing.reference_invoice_number_id,

        billing.invoiced_at,
        billing.refunded_at,
        billing.originally_invoiced_at,
        billing.period_start_at,
        billing.period_end_at,

    from int_billing as billing
    left join users
        on billing.user_id = users.user_id
)

select * from final
