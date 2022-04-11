with tickets as (
    select * from {{ ref('stg_support_tickets') }}
),

contacts as (
    select * from {{ ref('stg_support_contacts') }}
),

final as(
    select
        tickets.id,

        tickets.agent_name,
        tickets.type,
        tickets.source,
        tickets.source_label,
        tickets.status,
        tickets.status_label,
        tickets.priority,
        tickets.priority_label,
        tickets.to_emails,
        tickets.fwd_emails,
        tickets.tags,
        tickets.group_name,
        contacts.sector as contact_sector,
        contacts.role as contact_role,
        contacts.language_name as contact_language,
        contacts.plan as contact_plan,
        contacts.country_name as contact_country,

        tickets.contact_id,
        tickets.group_id,
        contacts.user_id,

        tickets.created_at,
        tickets.updated_at,
        tickets.agent_responded_at,
        tickets.contact_responded_at,
        tickets.first_responded_at,
        tickets.reopened_at,
        tickets.resolved_at,
        tickets.closed_at,
        contacts.registered_at as contact_registered_at

    from tickets
    left join contacts
        on tickets.contact_id = contacts.id
)

select * from final
