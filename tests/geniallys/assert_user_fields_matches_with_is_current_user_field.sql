-- Data relative to users should match the status of is_current_user

with geniallys as (
    select * from {{ ref('geniallys') }}
),

final as (
    select 
        genially_id, 
        user_id,
        is_current_user,
        user_plan,
        user_sector,
        user_role,
        user_market

    from geniallys
    where is_current_user = False 
            and (user_plan is not null
                or user_sector is not null 
                or user_role is not null
                or user_market is not null
            )
        or is_current_user = True
            and (user_plan is null
                or user_sector is null 
                or user_role is null
                or user_market is null
            )
)

select * from final