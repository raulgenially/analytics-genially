with licenses as (
    select * from {{ ref('src_genially_licenses') }}
),

invoices as (
    select * from {{ ref('src_genially_invoices') }}
),

users as (
    select * from {{ ref('users') }}
),

numbered_licenses as (
    select
        *,
        row_number() over(partition by user_id order by started_at desc) as rn
    from licenses
),

numbered_invoices as (
    select
        *,
        row_number() over(partition by subscription_id order by invoiced_at desc) as rn
    from invoices
),

final as (
    select
        users.email as Email,
        numbered_licenses.license_type as TypeLicense,
        users.plan as TypeSubscription,
        numbered_invoices.payment_platform as PaymentPlatform,
        users.role as Role,
        users.sector as Sector,
        date(users.last_access_at) as LastAccessTime,
        date(numbered_licenses.started_at) as DateStarter,
        users.country as Country,
        numbered_invoices.payer_email as PayerEmail,
        users.n_total_creations as Geniallys,

    from users
    left join numbered_licenses
        on users.user_id = numbered_licenses.user_id
    left join numbered_invoices
        on numbered_licenses.subscription_id = numbered_invoices.subscription_id
    where users.email in (
            "luiscebrian@genially.com",
            "antonio@genially.com",
            "raul@genial.ly"
        )
        -- We also check for null values here because we can have users
        -- that have not paid for their access, such as genially employees :)
        and (numbered_licenses.rn = 1 or numbered_licenses.rn is null)
        and (numbered_invoices.rn = 1 or numbered_invoices.rn is null)
    order by users.email
)

select * from final
