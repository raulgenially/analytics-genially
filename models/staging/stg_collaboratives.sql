with collaboratives as (
    select * from {{ ref('src_genially_collaboratives') }}
),

geniallys as (
    select * from {{ ref('stg_geniallys') }}
),

users as (
    select * from {{ ref('stg_users') }}
),

final as (
    select
        collaboratives.collaborative_id, 

        geniallys.is_published,
        geniallys.is_deleted,
        geniallys.is_in_social_profile,
        geniallys.is_visualized_last_90_days,
        geniallys.is_visualized_last_60_days,
        geniallys.is_visualized_last_30_days,
        users.is_social_profile_active as is_owner_social_profile_active,

        collaboratives.genially_id,        
        collaboratives.user_id,
        collaboratives.user_owner_id,

    from collaboratives
    inner join geniallys 
        on collaboratives.genially_id = geniallys.genially_id
    left join users
        on collaboratives.user_owner_id = users.user_id
    where collaboratives.user_owner_id = geniallys.user_id
        and collaboratives.user_id is not null
)

select * from final
