with users as (
  select * from {{ ref('adhoc_billing') }}
)

select * from users limit 0
