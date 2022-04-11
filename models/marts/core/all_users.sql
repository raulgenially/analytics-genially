with users as (
    select * from {{ ref('users') }}
),

deleted_users as (
    select * from {{ ref('deleted_users') }}
),

final as (
    select
        user_id,

        plan,
        subscription,
        sector,
        broad_sector,
        role,
        broad_role,
        country,
        country_name,

        registered_at,
        null as deleted_at

    from users

    union all

    select
        user_id,

        plan,
        subscription,
        sector,
        broad_sector,
        role,
        broad_role,
        country,
        country_name,

        registered_at,
        deleted_at

    from deleted_users
)

select * from final
