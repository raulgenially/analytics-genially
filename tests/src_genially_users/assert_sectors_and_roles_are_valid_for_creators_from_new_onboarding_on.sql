-- Sectors and roles of creators should have valid data from new onboarding date on.
{{
  config(
    severity='warn'
  )
}}

with geniallys as (
    select * from {{ ref('src_genially_geniallys') }}
),

users as (
  select * from {{ ref('src_genially_users') }}
),

final as (
    select
        geniallys.user_id,
        users.registered_at,
        users.last_access_at,
        count(geniallys.genially_id) as n_creations

    from geniallys
    inner join users
      on geniallys.user_id = users.user_id
    where geniallys.created_at > '{{ var('new_onboarding_date') }}'
        and (users.sector_code < 200
             or users.role_code < 100
             or users.role_code is null
             or users.sector_code is null)
    group by 1, 2, 3
    order by n_creations desc, last_access_at desc
)

select * from final
