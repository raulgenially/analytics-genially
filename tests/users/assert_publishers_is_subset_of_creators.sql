-- To ensure that publishers is a subset of creators (there are no publishers out of creators)
with creators as (
    select * from {{ ref('users') }}
    where is_creator = true
),

publishers as (
    select * from {{ ref('users') }}
    where is_publisher = true
),

final as (
    select
        publishers.user_id as publishers_user_id,
        publishers.email,
        creators.user_id as creators_user_id,

    from publishers
    left join creators
        on publishers.user_id = creators.user_id
    where creators.user_id is null
)

select * from final
