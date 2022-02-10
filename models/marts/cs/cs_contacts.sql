with contacts as (
    select * from {{ ref('src_freshdesk_contacts') }}
),

tickets as (
    select * from {{ ref('src_freshdesk_tickets') }}
),

tickets_count as (
    select
        contact_id,
        count(id) as n_tickets,

    from tickets
    group by 1
),

final as (
    select
        contacts.id,

        contacts.email,
        contacts.language,
        ifnull(tickets_count.n_tickets, 0) as n_tickets,

        contacts.created_at,
        contacts.updated_at

    from contacts
    left join tickets_count
        on contacts.id = tickets_count.contact_id
)

select * from final
