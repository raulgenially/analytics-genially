with billing as (
  select * from {{ ref('adhoc_billing') }}
)

select * from billing limit 0
