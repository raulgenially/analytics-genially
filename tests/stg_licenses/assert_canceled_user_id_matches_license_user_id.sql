-- The canceling user should be the same that purchased the license
with licenses as (
    select * from {{ ref('stg_licenses') }}
),

canceled_licenses as (
    select * from {{ ref('src_genially_canceled_licenses') }}
),

final as (
    select
        licenses.user_id,
        canceled_licenses.user_id as user_id_2
    from licenses
    inner join canceled_licenses
        on licenses.subscription_id = canceled_licenses.subscription_id
            and licenses.user_id != canceled_licenses.user_id
)

select * from final
