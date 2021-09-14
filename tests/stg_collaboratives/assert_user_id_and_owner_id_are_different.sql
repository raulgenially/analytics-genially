-- Ensure that the genially is collaborated with a user other than the owner

with collaboratives as (
    select * from {{ ref('stg_collaboratives') }}
),

final as (
    select
        collaborative_id,
        genially_id,
        user_id,
        user_owner_id

    from collaboratives
    where user_id = user_owner_id
)

select * from final
