-- From the new onboarding on ('2021-05-12'), sectors and roles of creators should have valid data.
{{
  config(
    severity='warn' 
  )
}}

with creators as (
    select * from {{ ref('creators') }}
),

final as (
    select
        user_id,
        sector, 
        sector_category,
        role,
        country,
        registered_at,
        last_access_at

    from creators
    where (registered_at >= '2021-05-12'
      or last_access_at >= '2021-05-12')
      and (sector like '%(old)%'
        or sector = '{{ var('not_selected') }}'
        or role like '%(old)%'
        or role = '{{ var('not_selected') }}')
    order by registered_at desc
)

select * from final
