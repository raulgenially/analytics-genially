-- Sectors and roles info of new users is associated to new codes.
with users as (
    select * from {{ ref('src_genially_users') }}
),

final as (
    select
        user_id,
        sector_code,
        sector,
        role_code,
        role,
        registered_at

    from users
    where registered_at > '{{ var('new_onboarding_date') }}'
        and (sector like '%(old)%'
            or role like '%(old)%')
    order by registered_at desc
)

select * from final
