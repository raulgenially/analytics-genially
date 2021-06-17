{{
    config(
        severity='warn'
    )
}}

with geniallys as (
    select * from {{ ref('stg_genially_geniallys') }}
),

users as (
    select * from {{ ref('stg_genially_users') }}
),

final as (
    select
        geniallys.genially_id,
        users.user_id,
        geniallys.subscription_plan,
        users.subscription_plan,
        geniallys.created_at,
        users.register_at

    from geniallys
    inner join users 
        on geniallys.user_id = users.user_id
    where geniallys.subscription_plan != users.subscription_plan
)

select * from final