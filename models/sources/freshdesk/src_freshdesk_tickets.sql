with tickets as (
    select * from {{ source('freshdesk', 'tickets') }}
),

sources as (
    select * from {{ ref('seed_freshdesk_ticket_sources') }}
),

statuses as (
    select * from {{ ref('seed_freshdesk_ticket_statuses') }}
),

priorities as (
    select * from {{ ref('seed_freshdesk_ticket_priorities') }}
),

agents as (
    select * from {{ ref('seed_freshdesk_ticket_agents') }}
),

base_tickets as (
    select
        tickets.*,
        sources.name as source_label,
        statuses.name as status_label,
        priorities.name as priority_label,
        agents.name as agent_name,

    from tickets
    left join sources
        on tickets.source = sources.code
    left join statuses
        on tickets.status = statuses.code
    left join priorities
        on tickets.priority = priorities.code
    left join agents
        on tickets.responder_id = agents.code
),

final as (
    select
        id,

        agent_name,
        type,
        source,
        source_label,
        status,
        status_label,
        priority,
        priority_label,
        {{ clean_ticket_emails('to_emails') }},
        {{ clean_ticket_emails('fwd_emails') }},
        tags,

        requester.id as contact_id,
        group_id,

        created_at,
        updated_at,
        -- stats
        stats.agent_responded_at,
        stats.requester_responded_at as contact_responded_at,
        stats.first_responded_at,
        stats.reopened_at,
        stats.resolved_at,
        stats.closed_at

    from base_tickets
)

select * from final
