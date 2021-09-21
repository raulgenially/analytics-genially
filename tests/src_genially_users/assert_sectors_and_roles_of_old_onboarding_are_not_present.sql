-- We shouldn't have sector and role names referring to the old onboarding from 12th May 2021 on.
{{
  config(
    severity='warn' 
  )
}}

with users as (
    select * from {{ ref('src_genially_users') }}
),

final as (
    select
        user_id,
        sector, 
        sector_category,
        role,
        country,
        registered_at

    from users
    where registered_at >= '2021-05-12' 
        and (sector like '%(old)%' 
            or role like '%(old)%')
    order by registered_at desc
)

select * from final
