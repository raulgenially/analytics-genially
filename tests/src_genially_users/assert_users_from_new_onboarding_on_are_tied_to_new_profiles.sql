-- Sectors and roles info of new users is associated to new codes.
-- Let's move on with this for now because this is not gonna be solved any time soon.
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
        sector_code,
        role_code,
        registered_at,
        last_access_at

    from users
    where registered_at > '{{ var('new_onboarding_date') }}'
        and (sector_code < 200 -- Refers to old onboarding codes
            or role_code < 100)
    order by registered_at desc
)

select * from final
