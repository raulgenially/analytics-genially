-- The transaction_id of invoices and refunds should be unique.
-- If there are duplicates in invoices, there should be a refund fixing this issue.
-- The duplicates in refunds should only happen in Stripe partial refunds of the same invoice.
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
),

dup_invoices as (
    {{ get_duplicated_records('invoices', 'transaction_id') }}
),

-- These are raw invoices with duplicates in transaction_id
potenial_unfixed_invoices as (
    select
        invoices.*

    from invoices
    inner join dup_invoices
        on invoices.transaction_id = dup_invoices.transaction_id
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

base_unfixed_refunds as (
    select
        refunds.*

    from refunds
    inner join dup_refunds
        on refunds.transaction_id = dup_refunds.transaction_id
),

-- Filter out those duplicated refunds from Stripe referencing the same invoice.
-- We group by transaction_id and reference_invoice_number_id because we want
-- to ensure that one transaction_id (although duplicated) is paired with the
-- same reference_invoice_number_id
dup_stripe_transactions as (
    select distinct
        transaction_id,
        reference_invoice_number_id,

    from base_unfixed_refunds
    where payment_platform = 'Stripe'
),

-- Now we get transaction_ids that only appeared once in our
-- transaction_id-reference_invoice_number_id pairs. If a transaction_id
-- appears in more than one pair it is a sign that something is wrong.
valid_dup_stripe_transactions as (
    select
        transaction_id,
        count(transaction_id) as n_records

    from dup_stripe_transactions
    group by 1
    having n_records = 1
),

-- Now we filter out those Stripe refunds whose transaction_id (although duplicated) is valid.
unfixed_refunds as (
    select
        refunds.*

    from base_unfixed_refunds as refunds
    left join valid_dup_stripe_transactions
        on refunds.transaction_id = valid_dup_stripe_transactions.transaction_id
    where valid_dup_stripe_transactions.transaction_id is null
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
