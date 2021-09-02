-- To ensure that publishers is a subset of creators (there are no publishers out of creators)
with creators as (
  select * from {{ ref('creators') }}
),

publishers as (
    select * from {{ ref('publishers') }}
),

final as (
    select
        publishers.user_id as publishers_user_id,
        publishers.email,
        creators.user_id as creators_user_id,
        creators.email

    from publishers
    left join creators
        on publishers.user_id = creators.user_id
    where creators.user_id is null
)

select * from final
