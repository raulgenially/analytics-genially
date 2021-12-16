with licenses as (
    select * from {{ ref('src_genially_licenses') }}
),

canceled_licenses as (
    select * from {{ ref('src_genially_canceled_licenses') }}
),

final as (
    select
        licenses.license_id,

        licenses.status,
        licenses.license_type,
        licenses.comments,
        licenses.user_ip,
        canceled_licenses.reason_code as canceled_reason_code,
        canceled_licenses.reason as canceled_reason,
        canceled_licenses.comment as canceled_comment,

        licenses.user_id,
        licenses.payer_id,
        licenses.transaction_id,
        licenses.subscription_id,

        licenses.started_at,
        licenses.finished_at,
        licenses.manual_finished_at,
        canceled_licenses.canceled_at

    from licenses
    left join canceled_licenses
        on licenses.subscription_id = canceled_licenses.subscription_id
)

select * from final
