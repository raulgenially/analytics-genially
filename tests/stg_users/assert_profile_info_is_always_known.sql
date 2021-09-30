-- The user profile info (sector and role) is known
with users as (
    select * from {{ ref('stg_users') }}
),

final as (
    select
        user_id,
        sector,
        role,
        country,
        registered_at

    from users
    where sector = '{{ var('unknown') }}'
        or role = '{{ var('unknown') }}'
)

select * from final
