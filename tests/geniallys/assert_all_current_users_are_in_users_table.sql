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
        genially_id, 
        user_id,
        is_current_user,
        
    from geniallys
    where is_current_user = True 
        and user_id not in (select user_id from users)
)

select * from final
