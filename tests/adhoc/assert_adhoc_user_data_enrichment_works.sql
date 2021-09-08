with users as (
  select * from {{ ref('adhoc_user_data_enrichment') }}
)

select * from users limit 0
