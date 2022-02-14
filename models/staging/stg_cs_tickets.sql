with tickets as (
    select * from {{ ref('src_freshdesk_tickets') }}
),

agent_groups as (
    select * from {{ ref('src_freshdesk_groups') }}
),


base_tickets as (
    select
        tickets.*,
        agent_groups.name as group_name,

    from tickets
    left join agent_groups
        on tickets.group_id = agent_groups.id
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
