-- TODO: remove once https://github.com/Genially/scrum-genially/issues/8259
-- is addressed
with collaboratives as (
    select * from {{ ref('src_genially_collaboratives') }}
),

users as (
    select * from {{ ref('src_genially_users') }}
),

final as (
    select
        collaboratives.*

    from collaboratives
    inner join users as owners
        on collaboratives.user_owner_id = owners.user_id
)

select * from final
