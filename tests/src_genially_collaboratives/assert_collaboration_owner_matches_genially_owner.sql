-- Collaboration owner should be the same as the genially owner

with collaboratives as (
    select * from {{ ref('src_genially_collaboratives') }}
),

geniallys as (
    select * from {{ ref('src_genially_geniallys') }}
),

final as (
    select
        collaboratives.*,
        geniallys.user_id as genially_user_id

    from collaboratives
    left join geniallys
        on collaboratives.genially_id = geniallys.genially_id
    where collaboratives.user_owner_id != geniallys.user_id
)

select * from final

