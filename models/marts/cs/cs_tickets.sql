with tickets as (
    select * from {{ ref('stg_cs_tickets') }}
)

select * from tickets
