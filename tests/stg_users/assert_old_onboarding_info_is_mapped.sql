-- We should not see user profile related to the old onboarding
{{
  config(
    severity='warn' 
  )
}}

with users as (
    select * from {{ ref('stg_users') }}
),

final as (
    select
        user_id,
        sector,
        role,
        registered_at

    from users
    where sector like '%(old)' 
        or role like '%(old)'
    order by registered_at desc
)

select * from final
