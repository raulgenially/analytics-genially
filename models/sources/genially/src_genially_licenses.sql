with license as (
    select * from {{ source('genially', 'license') }}
),

final as (
    select
        _id as license_id,

        status,
        typelicense as license_type,
        regexp_extract(typelicense, r'^\w+_(ANNUAL|MONTH)$') as recurrence,
        regexp_extract(typelicense, r'^(\w+)_(?:ANNUAL|MONTH)$') as plan,
        comments,
        ipuser as user_ip,

        iduser as user_id,
        payerid as payer_id,
        transactionid as transaction_id,
        subscriptionid as subscription_id,

        datestarter as started_at,
        datefinished as finished_at,
        manualdatefinished as manual_finished_at,

    from license
    where __hevo__marked_deleted = false
        and typelicense != '-1'
)

select * from final
