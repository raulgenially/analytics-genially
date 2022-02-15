with tickets as (
    select * from {{ ref('stg_cs_tickets') }}
),

contacts as (
    select * from {{ ref('stg_cs_contacts') }}
),

base_tickets as (
    select
        tickets.*,
        contacts.user_id,

    from tickets
    left join contacts
        on contacts.id = tickets.contact_id
),

final as (
    select
        id,

        type,
        source,
        source_label,
        status,
        status_label,
        priority,
        priority_label,
        to_emails,
        fwd_emails,
        tags,
        group_name,

        contact_id,
        user_id,
        group_id,

        created_at,
        updated_at,
        agent_responded_at,
        contact_responded_at,
        first_responded_at,
        reopened_at,
        resolved_at,
        closed_at,

    from base_tickets
)

select * from final
