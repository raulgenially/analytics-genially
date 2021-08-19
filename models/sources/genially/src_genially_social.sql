with social as (
    select * from {{ source('genially', 'social') }}
),

final as (
    select
        _id as social_profile_id,

        name as social_profile_name,

        ifnull(active, false) as is_active,
        
        iduser as user_id,
   
    from social
    where __hevo__marked_deleted = False
)

select * from final
