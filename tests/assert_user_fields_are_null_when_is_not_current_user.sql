-- Data relative to users should be NULL for not current users

with geniallys as (
    select * from {{ ref('geniallys') }}
),

final as (
    select 
        genially_id, 
        user_id,
        is_current_user,
        genially_plan,
        user_plan,
        user_sector,
        user_role,
        user_market

    from geniallys
    where is_current_user = False 
        and (user_plan is not null or user_sector is not null 
            or user_role is not null or user_market is not null) 
)

select * from final