-- The transaction_id of invoices and refunds should be unique.
-- If there are duplicates in invoices, there should be a refund fixing this issue.
-- The duplicates in refunds should not happen in any case.
with source as (
    select * from {{ ref('stg_invoices') }}
),

invoices as (
    select * from source
    where is_refund = false
),

refunds as (
    select * from source
    where is_refund = true
        -- This has been a special case where two refunds have been emitted
        -- with different value and same transaction_id.
        -- It should not happen moving forward
        and invoice_id not in ('6165c7b4055acc0de25640b0', '61c8c3b0b2a4cd0f878ffe13')
),

dup_invoices as (
    {{ get_duplicated_records('invoices', 'transaction_id') }}
),

-- These are raw invoices with duplicates in transaction_id
potenial_unfixed_invoices as (
    select 
        invoices.*
    
    from invoices
    left join dup_invoices
        on invoices.transaction_id = dup_invoices.transaction_id
    where dup_invoices.transaction_id is not null
),

-- These are the ones not fixed by a refund
unfixed_invoices as (
    select
        potenial_unfixed_invoices.*

    from potenial_unfixed_invoices
    left join refunds
        on potenial_unfixed_invoices.transaction_id = refunds.transaction_id
    where refunds.transaction_id is null
),

-- Now let's seek duplicates in refunds
dup_refunds as (
    {{ get_duplicated_records('refunds', 'transaction_id') }}
),

unfixed_refunds as (
    select
        refunds.*

    from refunds
    left join dup_refunds
        on refunds.transaction_id = dup_refunds.transaction_id
    where dup_refunds.transaction_id is not null
),

joined as (
    select
        'Invoices' as type,
        *
    
    from unfixed_invoices

    union all

    select
        'Refunds' as type,
        *
    
    from unfixed_refunds
),

final as (
    select * from joined
    order by type asc
)

select * from final
