with domain_emails  as (
  select * from {{ ref('adhoc_email_domain_signups') }}
)

select * from domain_emails limit 0
