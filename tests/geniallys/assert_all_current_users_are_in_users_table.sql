-- Geniallys from current users should be present in users table
-- That is, the foreign key should be valid for current users
with geniallys as (
    select * from {{ ref('geniallys') }}
),

users as (
        select * from {{ ref('users') }}
),

final as (
    select 
        geniallys.genially_id, 
        geniallys.user_id as genially_user_id,
        geniallys.is_from_current_user,
        users.user_id as user_id
        
    from geniallys
    left join users 
        on geniallys.user_id = users.user_id
    where geniallys.is_from_current_user = true 
        and users.user_id is null
)

select * from final
