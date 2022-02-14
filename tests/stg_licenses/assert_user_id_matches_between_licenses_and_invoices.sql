-- The user id in licenses and invoices should match
-- TODO: https://github.com/Genially/scrum-genially/issues/8586
{{
    config(
        severity="warn",
    )
}}
with licenses as (
    select * from {{ ref('stg_licenses') }}
),

invoices as (
    select * from {{ ref('stg_invoices') }}
),

final as (
    select
        licenses.license_id,
        licenses.status,
        licenses.user_id,
        licenses.started_at,
        invoices.invoice_id,
        invoices.is_refund as invoice_is_refund,
        invoices.user_id as invoice_user_id

    from licenses
    inner join invoices
        on licenses.subscription_id = invoices.subscription_id
    where licenses.user_id is distinct from invoices.user_id

)

select * from final
