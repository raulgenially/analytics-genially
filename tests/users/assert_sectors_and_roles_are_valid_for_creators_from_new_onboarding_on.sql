-- Sectors and roles of creators should have valid data from new onboarding date on.
{{
  config(
    severity='warn' 
  )
}}

with geniallys as (
    select * from {{ ref('geniallys') }}
),

final as (
    select
        user_id,
        count(genially_id) as n_creations

    from geniallys
    where created_at > '{{ var('new_onboarding_date') }}'
        and (user_sector like '%(old)%'
            or user_sector = '{{ var('not_selected') }}'
            or user_role like '%(old)%'
            or user_role = '{{ var('not_selected') }}')
    group by 1
    order by n_creations desc
)

select * from final
