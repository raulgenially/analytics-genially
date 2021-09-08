with users as (
  select * from {{ ref('adhoc_all_social_profiles') }}
)

select * from users limit 0
