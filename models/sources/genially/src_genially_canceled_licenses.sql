with canceled_licenses as (
    select * from {{ source('genially', 'canceledlicense') }}
    where __hevo__marked_deleted = false
),

cancel_codes as (
    select * from {{ ref('seed_cancel_license_reason_codes') }}
),

subscription_plans as (
    select * from {{ ref('seed_plan') }}
),

final as (
    select
        canceled_licenses._id as canceled_license_id,

        canceled_licenses.typesubscription as subscription_code,
        subscription_plans.plan as subscription_plan,
        cast(canceled_licenses.optionselected as int64) as reason_code,
        cancel_codes.name as reason,
        if(canceled_licenses.comment = '',
            null,
            canceled_licenses.comment
        ) as comment,

        canceled_licenses.iduser as user_id,
        canceled_licenses.idsubscription as subscription_id,

        canceled_licenses.canceldate as canceled_at

    from canceled_licenses
    left join cancel_codes
        on cast(canceled_licenses.optionselected as int64) = cancel_codes.code
    left join subscription_plans
        on canceled_licenses.typesubscription = subscription_plans.code
)

select * from final
