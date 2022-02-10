with tickets as (
    select * from {{ ref('src_freshdesk_tickets') }}
)

select * from tickets
