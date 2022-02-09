with licenses as (
    select * from {{ ref('src_genially_licenses') }}
    where status = 'Pending'
),

invoices as (
    select * from {{ ref('src_genially_invoices') }}
),

final as (
    select
        licenses.*
    from licenses
    inner join invoices
        on licenses.subscription_id = invoices.subscription_id
)

select * from final
