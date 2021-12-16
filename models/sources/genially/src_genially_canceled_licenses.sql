with src_canceled_licences as (
    select * from {{ source('genially', 'canceledlicense') }}
    where __hevo__marked_deleted = false
),

cancel_codes as (
    select * from {{ ref('seed_cancel_license_reason_codes') }}
),

canceled_licenses as (
    select
        src_canceled_licences._id as canceled_license_id,

        src_canceled_licences.typesubscription as subscription_type,
        cast(src_canceled_licences.optionselected as int64) as reason_code,
        cancel_codes.name as reason,
        if(src_canceled_licences.comment = '',
            null,
            src_canceled_licences.comment
        ) as comment,

        src_canceled_licences.iduser as user_id,
        src_canceled_licences.idsubscription as subscription_id,

        src_canceled_licences.canceldate as canceled_at

    from src_canceled_licences
    left join cancel_codes
        on cast(src_canceled_licences.optionselected as int64) = cancel_codes.code
)

select * from canceled_licenses
