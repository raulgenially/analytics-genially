with invoices as (
    select * from {{ ref('src_genially_invoices') }}
),

refunds as (
    select * from {{ ref('src_genially_refund_invoices') }}
),

-- We can have several refunds that point to the same invoice
unique_refunds as (
   {{ unique_records_by_column(
        'refunds',
        'reference_invoice_number_id',
        order_by='invoiced_at',
        dir='asc',
      )
   }}
),

invoices_normalized as (
    select
        invoices.invoice_id,

        invoices.description,
        invoices.payer_email,
        invoices.payer_name,
        invoices.payer_cif,
        invoices.payer_address,
        invoices.payer_country,
        invoices.total,
        invoices.total_euro,
        invoices.tax_rate,
        invoices.tax_key,
        invoices.currency,
        invoices.payment_platform,

        false as is_refund,
        invoices.is_valid_euvat_number,

        invoices.user_id,
        invoices.subscription_id,
        invoices.transaction_id,
        invoices.invoice_number_id,
        cast(null as string) as reference_invoice_number_id,

        invoices.invoiced_at,
        refunds.invoiced_at as refunded_at,
        invoices.invoiced_at as originally_invoiced_at,

    from invoices
    left join unique_refunds as refunds
        on invoices.invoice_number_id = refunds.reference_invoice_number_id
),

-- We can have several invoices with the same invoice_number
unique_invoices as (
   {{ unique_records_by_column(
        'invoices',
        'invoice_number_id',
        order_by='invoiced_at',
        dir='asc',
      )
   }}
),

refunds_normalized as (
    select
        refunds.invoice_id,

        refunds.description,
        refunds.payer_email,
        refunds.payer_name,
        refunds.payer_cif,
        refunds.payer_address,
        refunds.payer_country,
        refunds.total,
        refunds.total_euro,
        refunds.tax_rate,
        refunds.tax_key,
        refunds.currency,
        refunds.payment_platform,

        true as is_refund,
        refunds.is_valid_euvat_number,

        refunds.user_id,
        refunds.subscription_id,
        refunds.transaction_id,
        refunds.invoice_number_id,
        refunds.reference_invoice_number_id,

        refunds.invoiced_at,
        cast(null as timestamp) as refunded_at,
        invoices.invoiced_at as originally_invoiced_at,

    from refunds
    left join unique_invoices as invoices
        on refunds.reference_invoice_number_id = invoices.invoice_number_id
),

unioned as (
    select * from invoices_normalized
    union all
    select * from refunds_normalized
),

final as (
    select
        {{ dbt_utils.surrogate_key([
            'invoice_id',
            'is_refund'
        ]) }} as id,
        *
    from unioned
)

select * from final
