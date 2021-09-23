with user_licenses as (
  select * from {{ ref('adhoc_licenses_metadata') }}
)

select * from user_licenses limit 0
