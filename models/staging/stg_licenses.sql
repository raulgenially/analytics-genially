with licenses as (
    select * from {{ ref('src_genially_licenses') }}
),

canceled_licenses as (
    select * from {{ ref('src_genially_canceled_licenses') }}
),

final as (
    select
        licenses.license_id,

        {{ map_license_status(
            'licenses.status',
            'canceled_licenses.reason_code'
           )
        }} as status,
        licenses.license_type,
        licenses.recurrence,
        licenses.plan,
        licenses.comments,

        licenses.user_id,
        licenses.payer_id,
        licenses.subscription_id,

        licenses.started_at,
        licenses.finished_at,
        canceled_licenses.canceled_at

    from licenses
    left join canceled_licenses
        on licenses.subscription_id = canceled_licenses.subscription_id
)

select * from final
