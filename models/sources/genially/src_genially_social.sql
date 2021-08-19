with social as (
    select * from {{ source('genially', 'social') }}
),

final as (
    select
        _id as socialprofile_id,

        ifnull(active, false) as is_active,
        name as social_profiile_name,
        
        iduser as user_id,
   
    from social
    where __hevo__marked_deleted = False
)

select * from final
