{{
    config(
        materialized='table'
    )
}}

with geniallys as (
    select * from {{ ref('src_genially_geniallys') }}
),

users as (
    select * from {{ ref('src_genially_users') }}
),

-- Note that there are geniallys from people who are not users any more.

final as (
    select
        geniallys.genially_id,
        geniallys.genially_type,
        geniallys.subscription_plan as genially_plan,
        geniallys.is_deleted,
        geniallys.user_id as genially_user_id,
        geniallys.created_at,

        users.user_id as user_id,
        users.subscription_plan as user_plan,
        users.sector,
        users.role,
        users.country as market,
        users.is_validated as user_is_validated,
        users.register_at as user_register_at,
        users.last_access_at as user_last_access_at

    from geniallys
    left join users
        on geniallys.user_id = users.user_id
)

select * from final