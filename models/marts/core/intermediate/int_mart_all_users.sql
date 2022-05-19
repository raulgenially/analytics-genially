{{
    config(
        materialized='view'
    )
}}

with deleted_users as (
    select * from {{ ref('stg_users') }}
    where is_deleted = true
),

users as (
    select * from {{ ref('stg_users') }}
    where is_deleted = false
),

phishing as (
    select * from {{ ref('src_miscellaneous_phishing_users') }}
),

deleted_users_cleaned as (
    select
        deleted_users.*

    from deleted_users
    left join phishing
        on deleted_users.user_id = phishing.user_id
    where phishing.user_id is null
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
        updated_at as deleted_at

    from deleted_users_cleaned
)

select * from final
