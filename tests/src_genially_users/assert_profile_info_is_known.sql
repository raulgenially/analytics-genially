-- The user profile info (sector and role) is known.
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
        country,
        registered_at

    from users
    where sector = '{{ var('unknown') }}'
        or role = '{{ var('unknown') }}'
    order by registered_at desc
)

select * from final
