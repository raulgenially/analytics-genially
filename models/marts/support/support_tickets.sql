with tickets as (
    select * from {{ ref('stg_support_tickets') }}
)

select * from tickets
