with licenses as (
    select * from {{ ref('stg_licenses') }}
),

invoices as (
    select * from {{ ref('stg_invoices') }}
    where is_refund = false
),

users as (
    select * from {{ ref('users') }}
),

last_licenses as (
   {{ unique_records_by_column(
        'licenses',
        'user_id',
        order_by='started_at',
        dir='desc',
      )
   }}
),

last_invoices as (
   {{ unique_records_by_column(
        'invoices',
        'subscription_id',
        order_by='invoiced_at',
        dir='desc',
      )
   }}
),

final as (
    select
        users.email as Email,
        users.nickname as Nickname,
        users.plan as Plan,
        users.role as Role,
        users.sector as Sector,
        users.country as Country,
        date(users.last_access_at) as LastAccessTime,
        last_licenses.license_type as LicenseType,
        date(last_licenses.started_at) as LicenseStartDate,
        last_invoices.payment_platform as PaymentPlatform,
        last_invoices.payer_email as PayerEmail,
        users.n_total_creations as Geniallys,

    from users
    left join last_licenses
        on users.user_id = last_licenses.user_id
    left join last_invoices
        on last_licenses.subscription_id = last_invoices.subscription_id
)

select * from final
