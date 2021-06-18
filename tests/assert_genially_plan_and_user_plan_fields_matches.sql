-- Info as to plan in both geniallys and users should match.

with geniallys_users as (
    select * from {{ ref('stg_geniallys_users_joined') }}
),

final as (
    select
        genially_id,
        user_id,
        genially_plan,
        user_plan,
        created_at,
        user_register_at

    from geniallys_users
    where genially_plan != user_plan
)

select * from final