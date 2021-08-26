with collaboratives as (
    select * from {{ ref('src_genially_collaboratives') }}
),

geniallys as (
    select * from {{ ref('stg_geniallys') }}
),

final as (
    select
        collaboratives.collaborative_id, 

        geniallys.is_published,
        geniallys.is_deleted,
        geniallys.is_in_social_profile,

        collaboratives.genially_id,        
        collaboratives.user_id,
        collaboratives.user_owner_id,

    from collaboratives
    inner join geniallys 
        on collaboratives.genially_id = geniallys.genially_id
    where collaboratives.user_owner_id = geniallys.user_id
        and collaboratives.user_id is not null
)

select * from final
