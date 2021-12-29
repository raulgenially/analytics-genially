with collaboratives as (
    select * from {{ ref('stg_collaboratives') }}
)

select * from collaboratives
