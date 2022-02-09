with licenses as (
    select * from {{ ref('src_genially_licenses') }}
),

canceled_licenses as (
    select * from {{ ref('src_genially_canceled_licenses') }}
),

base_licenses as (
    select
        licenses.*,
        {{
            map_license_status(
                'licenses.status',
                'canceled_licenses.reason_code'
           )
        }} as subscription_status,
        canceled_licenses.canceled_at

    from licenses
    left join canceled_licenses
        on licenses.subscription_id = canceled_licenses.subscription_id
),

final as (
    select
        licenses.license_id,

        licenses.subscription_status as status,
        licenses.license_type,
        licenses.recurrence,
        licenses.plan,
        licenses.comments,

        licenses.user_id,
        licenses.payer_id,
        licenses.subscription_id,

        licenses.subscription_status in ('Active', 'Canceled') as is_active,

        licenses.started_at,
        licenses.finished_at,
        licenses.canceled_at

    from base_licenses as licenses
    left join canceled_licenses
        on licenses.subscription_id = canceled_licenses.subscription_id
)

select * from final
