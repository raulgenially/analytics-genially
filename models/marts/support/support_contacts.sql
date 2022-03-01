with contacts as (
    select * from {{ ref('stg_support_contacts') }}
),

tickets as (
    select * from {{ ref('stg_support_tickets') }}
),

tickets_count as (
    select
        contact_id,
        count(id) as n_tickets,

    from tickets
    group by 1
),

base_contacts as (
    select
        contacts.*,
        ifnull(tickets_count.n_tickets, 0) as n_tickets,

    from contacts
    left join tickets_count
        on contacts.id = tickets_count.contact_id
),

final as (
    select
        id,

        email,
        language,
        language_name,
        n_tickets,
        plan,
        subscription_period,
        country,
        country_name,
        sector_code,
        sector,
        role_code,
        role,
        city,
        n_total_creations,

        user_id,

        created_at,
        updated_at,
        subscription_started_at,
        last_login_at,
        registered_at

    from base_contacts
)

select * from final
