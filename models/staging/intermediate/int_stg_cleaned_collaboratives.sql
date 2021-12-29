-- TODO: remove once https://github.com/Genially/scrum-genially/issues/8259
-- is addressed
with collaboratives as (
    select * from {{ ref('src_genially_collaboratives') }}
),

geniallys as (
    select * from {{ ref('src_genially_geniallys') }}
),

users as (
    select * from {{ ref('src_genially_users') }}
),

final as (
    select
        collaboratives.*

    from collaboratives
    inner join geniallys
        on collaboratives.genially_id = geniallys.genially_id
    inner join users as owners
        on collaboratives.user_owner_id = owners.user_id
    where collaboratives.user_owner_id = geniallys.user_id
)

select * from final
